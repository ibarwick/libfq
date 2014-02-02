/*-------------------------------------------------------------------------
 *
 * fqexpbuffer.h
 *
 * Declarations/definitions for "FQExpBuffer" functions.
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
 *-------------------------------------------------------------------------
 */
#ifndef LIBFQFQEXPBUFFER_H
#define LIBFQFQEXPBUFFER_H

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*-------------------------
 * FQExpBufferData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with malloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *
 * An exception occurs if we failed to allocate enough memory for the string
 * buffer.	In that case data points to a statically allocated empty string,
 * and len = maxlen = 0.
 *-------------------------
 */
typedef struct FQExpBufferData
{
	char	   *data;
	size_t		len;
	size_t		maxlen;
} FQExpBufferData;

typedef FQExpBufferData *FQExpBuffer;

/*------------------------
 * Test for a broken (out of memory) FQExpBuffer.
 * When a buffer is "broken", all operations except resetting or deleting it
 * are no-ops.
 *------------------------
 */
#define FQExpBufferBroken(str)	\
	((str) == NULL || (str)->maxlen == 0)

/*------------------------
 * Same, but for use when using a static or local FQExpBufferData struct.
 * For that, a null-pointer test is useless and may draw compiler warnings.
 *------------------------
 */
#define FQExpBufferDataBroken(buf)	\
	((buf).maxlen == 0)

/*------------------------
 * Initial size of the data buffer in a FQExpBuffer.
 * NB: this must be large enough to hold error messages that might
 * be returned by PQrequestCancel().
 *------------------------
 */
#define INITIAL_EXPBUFFER_SIZE	256

/*------------------------
 * There are two ways to create a FQExpBuffer object initially:
 *
 * FQExpBuffer stringptr = createFQExpBuffer();
 *		Both the FQExpBufferData and the data buffer are malloc'd.
 *
 * FQExpBufferData string;
 * initFQExpBuffer(&string);
 *		The data buffer is malloc'd but the FQExpBufferData is presupplied.
 *		This is appropriate if the FQExpBufferData is a field of another
 *		struct.
 *-------------------------
 */

/*------------------------
 * createFQExpBuffer
 * Create an empty 'FQExpBufferData' & return a pointer to it.
 */
extern FQExpBuffer createFQExpBuffer(void);

/*------------------------
 * initFQExpBuffer
 * Initialize a FQExpBufferData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void initFQExpBuffer(FQExpBuffer str);

/*------------------------
 * To destroy a FQExpBuffer, use either:
 *
 * destroyFQExpBuffer(str);
 *		free()s both the data buffer and the FQExpBufferData.
 *		This is the inverse of createFQExpBuffer().
 *
 * termFQExpBuffer(str)
 *		free()s the data buffer but not the FQExpBufferData itself.
 *		This is the inverse of initFQExpBuffer().
 *
 * NOTE: some routines build up a string using FQExpBuffer, and then
 * release the FQExpBufferData but return the data string itself to their
 * caller.	At that point the data string looks like a plain malloc'd
 * string.
 */
extern void destroyFQExpBuffer(FQExpBuffer str);
extern void termFQExpBuffer(FQExpBuffer str);

/*------------------------
 * resetFQExpBuffer
 *		Reset a FQExpBuffer to empty
 *
 * Note: if possible, a "broken" FQExpBuffer is returned to normal.
 */
extern void resetFQExpBuffer(FQExpBuffer str);

/*------------------------
 * enlargeFQExpBuffer
 * Make sure there is enough space for 'needed' more bytes in the buffer
 * ('needed' does not include the terminating null).
 *
 * Returns 1 if OK, 0 if failed to enlarge buffer.	(In the latter case
 * the buffer is left in "broken" state.)
 */
extern int	enlargeFQExpBuffer(FQExpBuffer str, size_t needed);

/*------------------------
 * printfFQExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and insert it into str.	More space is allocated to str if necessary.
 * This is a convenience routine that does the same thing as
 * resetFQExpBuffer() followed by appendFQExpBuffer().
 */
extern void
printfFQExpBuffer(FQExpBuffer str, const char *fmt,...);

/*------------------------
 * appendFQExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void
appendFQExpBuffer(FQExpBuffer str, const char *fmt,...);

/*------------------------
 * appendFQExpBufferStr
 * Append the given string to a FQExpBuffer, allocating more space
 * if necessary.
 */
extern void appendFQExpBufferStr(FQExpBuffer str, const char *data);

/*------------------------
 * appendFQExpBufferChar
 * Append a single byte to str.
 * Like appendFQExpBuffer(str, "%c", ch) but much faster.
 */
extern void appendFQExpBufferChar(FQExpBuffer str, char ch);

/*------------------------
 * appendBinaryFQExpBuffer
 * Append arbitrary binary data to a FQExpBuffer, allocating more space
 * if necessary.
 */
extern void appendBinaryFQExpBuffer(FQExpBuffer str,
						const char *data, size_t datalen);

#endif   /* LIBFQEXPBUFFER_H */
