/*-------------------------------------------------------------------------
 *
 * fqexpbuffer.c
 *
 * FQExpBuffer provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with malloc().
 *
 * This module is essentially a copy of PostgreSQL's pqexpbuffer.c; see:
 *
 *   http://git.postgresql.org/gitweb/?p=postgresql.git;f=src/interfaces/libpq/pqexpbuffer.c;hb=HEAD
 *
 * Note that this library relies on vsnprintf() and will not work if a
 * suitable implementation is not available.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/fqexpbuffer.c
 *
 *-------------------------------------------------------------------------
 */

#include "libfq.h"

#include <limits.h>

#include "libfq-expbuffer.h"


/* All "broken" FQExpBuffers point to this string. */
static const char oom_buffer[1] = "";


/*
 * markFQExpBufferBroken
 *
 * Put a FQExpBuffer in "broken" state if it isn't already.
 */
static void
markFQExpBufferBroken(FQExpBuffer str)
{
	if (str->data != oom_buffer)
		free(str->data);

	/*
	 * Casting away const here is a bit ugly, but it seems preferable to not
	 * marking oom_buffer const.  We want to do that to encourage the compiler
	 * to put oom_buffer in read-only storage, so that anyone who tries to
	 * scribble on a broken FQExpBuffer will get a failure.
	 */
	str->data = (char *) oom_buffer;
	str->len = 0;
	str->maxlen = 0;
}

/*
 * createFQExpBuffer
 *
 * Create an empty 'FQExpBufferData' & return a pointer to it.
 */
FQExpBuffer
createFQExpBuffer(void)
{
	FQExpBuffer res;

	res = (FQExpBuffer) malloc(sizeof(FQExpBufferData));
	if (res != NULL)
		initFQExpBuffer(res);

	return res;
}

/*
 * initFQExpBuffer
 *
 * Initialize a FQExpBufferData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initFQExpBuffer(FQExpBuffer str)
{
	str->data = (char *) malloc(INITIAL_EXPBUFFER_SIZE);
	if (str->data == NULL)
	{
		str->data = (char *) oom_buffer;		/* see comment above */
		str->maxlen = 0;
		str->len = 0;
	}
	else
	{
		str->maxlen = INITIAL_EXPBUFFER_SIZE;
		str->len = 0;
		str->data[0] = '\0';
	}
}

/*
 * destroyFQExpBuffer(str);
 *
 *      free()s both the data buffer and the FQExpBufferData.
 *      This is the inverse of createFQExpBuffer().
 */
void
destroyFQExpBuffer(FQExpBuffer str)
{
	if (str)
	{
		termFQExpBuffer(str);
		free(str);
	}
}

/*
 * termFQExpBuffer(str)
 *      free()s the data buffer but not the FQExpBufferData itself.
 *      This is the inverse of initFQExpBuffer().
 */
void
termFQExpBuffer(FQExpBuffer str)
{
	if (str->data != oom_buffer)
		free(str->data);
	/* just for luck, make the buffer validly empty. */
	str->data = (char *) oom_buffer;	/* see comment above */
	str->maxlen = 0;
	str->len = 0;
}

/*
 * resetFQExpBuffer
 *      Reset a FQExpBuffer to empty
 *
 * Note: if possible, a "broken" FQExpBuffer is returned to normal.
 */
void
resetFQExpBuffer(FQExpBuffer str)
{
	if (str)
	{
		if (str->data != oom_buffer)
		{
			str->len = 0;
			str->data[0] = '\0';
		}
		else
		{
			/* try to reinitialize to valid state */
			initFQExpBuffer(str);
		}
	}
}

/*
 * enlargeFQExpBuffer
 * Make sure there is enough space for 'needed' more bytes in the buffer
 * ('needed' does not include the terminating null).
 *
 * Returns 1 if OK, 0 if failed to enlarge buffer.  (In the latter case
 * the buffer is left in "broken" state.)
 */
int
enlargeFQExpBuffer(FQExpBuffer str, size_t needed)
{
	size_t		newlen;
	char	   *newdata;

	if (FQExpBufferBroken(str))
		return 0;				/* already failed */

	/*
	 * Guard against ridiculous "needed" values, which can occur if we're fed
	 * bogus data.	Without this, we can get an overflow or infinite loop in
	 * the following.
	 */
	if (needed >= ((size_t) INT_MAX - str->len))
	{
		markFQExpBufferBroken(str);
		return 0;
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= INT_MAX */

	if (needed <= str->maxlen)
		return 1;				/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = (str->maxlen > 0) ? (2 * str->maxlen) : 64;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to INT_MAX in case we went past it.  Note we are assuming here
	 * that INT_MAX <= UINT_MAX/2, else the above loop could overflow.	We
	 * will still have newlen >= needed.
	 */
	if (newlen > (size_t) INT_MAX)
		newlen = (size_t) INT_MAX;

	newdata = (char *) realloc(str->data, newlen);
	if (newdata != NULL)
	{
		str->data = newdata;
		str->maxlen = newlen;
		return 1;
	}

	markFQExpBufferBroken(str);
	return 0;
}

/*
 * printfFQExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and insert it into str.  More space is allocated to str if necessary.
 * This is a convenience routine that does the same thing as
 * resetFQExpBuffer() followed by appendFQExpBuffer().
 */
void
printfFQExpBuffer(FQExpBuffer str, const char *fmt,...)
{
	va_list		args;
	size_t		avail;
	int			nprinted;

	resetFQExpBuffer(str);

	if (FQExpBufferBroken(str))
		return;					/* already failed */

	for (;;)
	{
		/*
		 * Try to format the given string into the available space; but if
		 * there's hardly any space, don't bother trying, just fall through to
		 * enlarge the buffer first.
		 */
		if (str->maxlen > str->len + 16)
		{
			avail = str->maxlen - str->len - 1;
			va_start(args, fmt);
			nprinted = vsnprintf(str->data + str->len, avail,
								 fmt, args);
			va_end(args);

			/*
			 * Note: some versions of vsnprintf return the number of chars
			 * actually stored, but at least one returns -1 on failure. Be
			 * conservative about believing whether the print worked.
			 */
			if (nprinted >= 0 && nprinted < (int) avail - 1)
			{
				/* Success.	 Note nprinted does not include trailing null. */
				str->len += nprinted;
				break;
			}
		}
		/* Double the buffer size and try again. */
		if (!enlargeFQExpBuffer(str, str->maxlen))
			return;				/* oops, out of memory */
	}
}

/*
 * appendFQExpBuffer
 *
 * Format text data under the control of fmt (an sprintf-like format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void
appendFQExpBuffer(FQExpBuffer str, const char *fmt,...)
{
	va_list		args;
	size_t		avail;
	int			nprinted;

	if (FQExpBufferBroken(str))
		return;					/* already failed */

	for (;;)
	{
		/*
		 * Try to format the given string into the available space; but if
		 * there's hardly any space, don't bother trying, just fall through to
		 * enlarge the buffer first.
		 */
		if (str->maxlen > str->len + 16)
		{
			avail = str->maxlen - str->len - 1;
			va_start(args, fmt);
			nprinted = vsnprintf(str->data + str->len, avail,
								 fmt, args);
			va_end(args);

			/*
			 * Note: some versions of vsnprintf return the number of chars
			 * actually stored, but at least one returns -1 on failure. Be
			 * conservative about believing whether the print worked.
			 */
			if (nprinted >= 0 && nprinted < (int) avail - 1)
			{
				/* Success.	 Note nprinted does not include trailing null. */
				str->len += nprinted;
				break;
			}
		}
		/* Double the buffer size and try again. */
		if (!enlargeFQExpBuffer(str, str->maxlen))
			return;				/* oops, out of memory */
	}
}


/*
 * appendFQExpBufferStr
 * Append the given string to a FQExpBuffer, allocating more space
 * if necessary.
 */
void
appendFQExpBufferStr(FQExpBuffer str, const char *data)
{
	appendBinaryFQExpBuffer(str, data, strlen(data));
}

/*
 * appendFQExpBufferChar
 * Append a single byte to str.
 * Like appendFQExpBuffer(str, "%c", ch) but much faster.
 */
void
appendFQExpBufferChar(FQExpBuffer str, char ch)
{
	/* Make more room if needed */
	if (!enlargeFQExpBuffer(str, 1))
		return;

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

/*
 * appendBinaryFQExpBuffer
 *
 * Append arbitrary binary data to a FQExpBuffer, allocating more space
 * if necessary.
 */
void
appendBinaryFQExpBuffer(FQExpBuffer str, const char *data, size_t datalen)
{
	/* Make more room if needed */
	if (!enlargeFQExpBuffer(str, datalen))
		return;

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data...
	 */
	str->data[str->len] = '\0';
}
