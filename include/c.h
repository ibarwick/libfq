#ifndef C_H
#define C_H


#include <stdlib.h>

#ifndef BOOL
#define BOOL
typedef char bool;


#ifndef true
#define true    ((bool) 1)
#endif

#ifndef false
#define false   ((bool) 0)
#endif

typedef bool *BoolPtr;

#ifndef TRUE
#define TRUE    1
#endif

#ifndef FALSE
#define FALSE   0
#endif
#endif

/*
 * lengthof
 *              Number of elements in an array.
 */
#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))

/*
 * endof
 *              Address of the element one past the last in an array.
 */
#define endof(array)    (&(array)[lengthof(array)])

#define HIGHBIT					(0x80)
#define IS_HIGHBIT_SET(ch)		((unsigned char)(ch) & HIGHBIT)

typedef unsigned char uint8;    /* == 8 bits */
typedef unsigned short uint16;  /* == 16 bits */
typedef unsigned int uint32;    /* == 32 bits */

/*
 * bitsN
 *              Unit of bitwise operation, AT LEAST N BITS IN SIZE.
 */
typedef uint8 bits8;                    /* >= 8 bits */
typedef uint16 bits16;                  /* >= 16 bits */
typedef uint32 bits32;                  /* >= 32 bits */

#endif   /* C_H */
