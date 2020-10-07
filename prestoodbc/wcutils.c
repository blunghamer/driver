#include "wcutils.h"

/**
 * Return length of UNICODE string.
 * @param str UNICODE string
 * @result length of string in characters
 */

int
uc_strlen(SQLWCHAR *str)
{
    int len = 0;

    if (str)
    {
        while (*str)
        {
            ++len;
            ++str;
        }
    }
    return len;
}

/**
 * Make UTF8 string from UNICODE string.
 * @param str UNICODE string to be converted
 * @param len length of UNICODE string in bytes
 * @return alloc'ed UTF8 string to be free'd by uc_free()
 */

char *
uc_to_utf(SQLWCHAR *str, int len)
{
    int i;
    char *cp, *ret = NULL;

    if (!str)
    {
        return ret;
    }
    if (len == SQL_NTS)
    {
        len = uc_strlen(str);
    }
    else
    {
        len = len / sizeof(SQLWCHAR);
    }
    cp = malloc(len *6 + 1 );
    //cp = xmalloc(len * 6 + 1);
    if (!cp)
    {
        return ret;
    }
    ret = cp;
    for (i = 0; i < len; i++)
    {
        unsigned long c = str[i];

        if (sizeof(SQLWCHAR) == 2 * sizeof(char))
        {
            c &= 0xffff;
        }
        if (c < 0x80)
        {
            *cp++ = c;
        }
        else if (c < 0x800)
        {
            *cp++ = 0xc0 | ((c >> 6) & 0x1f);
            *cp++ = 0x80 | (c & 0x3f);
        }
        else if (c < 0x10000)
        {
            if (sizeof(SQLWCHAR) == 2 * sizeof(char) &&
                c >= 0xd800 && c <= 0xdbff && i + 1 < len)
            {
                unsigned long c2 = str[i + 1] & 0xffff;

                if (c2 >= 0xdc00 && c2 <= 0xdfff)
                {
                    c = (((c & 0x3ff) << 10) | (c2 & 0x3ff)) + 0x10000;
                    *cp++ = 0xf0 | ((c >> 18) & 0x07);
                    *cp++ = 0x80 | ((c >> 12) & 0x3f);
                    *cp++ = 0x80 | ((c >> 6) & 0x3f);
                    *cp++ = 0x80 | (c & 0x3f);
                    ++i;
                    continue;
                }
            }
            *cp++ = 0xe0 | ((c >> 12) & 0x0f);
            *cp++ = 0x80 | ((c >> 6) & 0x3f);
            *cp++ = 0x80 | (c & 0x3f);
        }
        else if (c <= 0x10ffff)
        {
            *cp++ = 0xf0 | ((c >> 18) & 0x07);
            *cp++ = 0x80 | ((c >> 12) & 0x3f);
            *cp++ = 0x80 | ((c >> 6) & 0x3f);
            *cp++ = 0x80 | (c & 0x3f);
        }
    }
    *cp = '\0';
    return ret;
}

char *
uc_to_utf_c(SQLWCHAR *str, int len)
{
    if (len != SQL_NTS)
    {
        len = len * sizeof(SQLWCHAR);
    }
    return uc_to_utf(str, len);
}

/**
 * Free converted UTF8 or UNICODE string.
 * @param str string to be free'd
 */

void
uc_free(void *str)
{
    if (str) {
    	free(str);
    }
}
