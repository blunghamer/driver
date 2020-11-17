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


#if defined(_WIN32) || defined(_WIN64)

/**
 * Convert multibyte, current code page string to UTF8 string,
 * @param str multibyte string to be converted
 * @param len length of multibyte string
 * @return alloc'ed UTF8 string to be free'd by uc_free()
 */

char *
wmb_to_utf(char *str, int len)
{
    WCHAR *wstr;
    OSVERSIONINFO ovi;
    int nchar, is2k, cp = CP_OEMCP;

    ovi.dwOSVersionInfoSize = sizeof (ovi);
    GetVersionEx(&ovi);
    is2k = ovi.dwPlatformId == VER_PLATFORM_WIN32_NT && ovi.dwMajorVersion > 4;
    if (AreFileApisANSI()) {
	cp = is2k ? CP_THREAD_ACP : CP_ACP;
    }
    nchar = MultiByteToWideChar(cp, 0, str, len, NULL, 0);
    wstr = xmalloc((nchar + 1) * sizeof (WCHAR));
    if (!wstr) {
	return NULL;
    }
    wstr[0] = 0;
    nchar = MultiByteToWideChar(cp, 0, str, len, wstr, nchar);
    wstr[nchar] = 0;
    str = xmalloc((nchar + 1) * 7);
    if (!str) {
	xfree(wstr);
	return NULL;
    }
    str[0] = '\0';
    nchar = WideCharToMultiByte(CP_UTF8, 0, wstr, -1, str, nchar * 7, 0, 0);
    str[nchar] = '\0';
    xfree(wstr);
    return str;
}

#ifndef WINTERFACE

/**
 * Convert multibyte, current code page string to UTF8 string,
 * @param str multibyte string to be converted
 * @param len length of multibyte string
 * @return alloc'ed UTF8 string to be free'd by uc_free()
 */

char *
wmb_to_utf_c(char *str, int len)
{
    if (len == SQL_NTS) {
	len = strlen(str);
    }
    return wmb_to_utf(str, len);
}

#endif


/**
 * Convert UTF8 string to multibyte, current code page string,
 * @param str UTF8 string to be converted
 * @param len length of UTF8 string
 * @return alloc'ed multibyte string to be free'd by uc_free()
 */

char *
utf_to_wmb(char *str, int len)
{
    WCHAR *wstr;
    OSVERSIONINFO ovi;
    int nchar, is2k, cp = CP_OEMCP;

    ovi.dwOSVersionInfoSize = sizeof (ovi);
    GetVersionEx(&ovi);
    is2k = ovi.dwPlatformId == VER_PLATFORM_WIN32_NT && ovi.dwMajorVersion > 4;
    if (AreFileApisANSI()) {
	cp = is2k ? CP_THREAD_ACP : CP_ACP;
    }
    nchar = MultiByteToWideChar(CP_UTF8, 0, str, len, NULL, 0);
    wstr = xmalloc((nchar + 1) * sizeof (WCHAR));
    if (!wstr) {
	return NULL;
    }
    wstr[0] = 0;
    nchar = MultiByteToWideChar(CP_UTF8, 0, str, len, wstr, nchar);
    wstr[nchar] = 0;
    str = xmalloc((nchar + 1) * 7);
    if (!str) {
	xfree(wstr);
	return NULL;
    }
    str[0] = '\0';
    nchar = WideCharToMultiByte(cp, 0, wstr, -1, str, nchar * 7, 0, 0);
    str[nchar] = '\0';
    xfree(wstr);
    return str;
}

#ifdef WINTERFACE

/**
 * Convert multibyte, current code page string to UNICODE string,
 * @param str multibyte string to be converted
 * @param len length of multibyte string
 * @return alloc'ed UNICODE string to be free'd by uc_free()
 */

WCHAR *
wmb_to_uc(char *str, int len)
{
    WCHAR *wstr;
    OSVERSIONINFO ovi;
    int nchar, is2k, cp = CP_OEMCP;

    ovi.dwOSVersionInfoSize = sizeof (ovi);
    GetVersionEx(&ovi);
    is2k = ovi.dwPlatformId == VER_PLATFORM_WIN32_NT && ovi.dwMajorVersion > 4;
    if (AreFileApisANSI()) {
	cp = is2k ? CP_THREAD_ACP : CP_ACP;
    }
    nchar = MultiByteToWideChar(cp, 0, str, len, NULL, 0);
    wstr = xmalloc((nchar + 1) * sizeof (WCHAR));
    if (!wstr) {
	return NULL;
    }
    wstr[0] = 0;
    nchar = MultiByteToWideChar(cp, 0, str, len, wstr, nchar);
    wstr[nchar] = 0;
    return wstr;
}

/**
 * Convert UNICODE string to multibyte, current code page string,
 * @param str UNICODE string to be converted
 * @param len length of UNICODE string
 * @return alloc'ed multibyte string to be free'd by uc_free()
 */

char *
uc_to_wmb(WCHAR *wstr, int len)
{
    char *str;
    OSVERSIONINFO ovi;
    int nchar, is2k, cp = CP_OEMCP;

    ovi.dwOSVersionInfoSize = sizeof (ovi);
    GetVersionEx(&ovi);
    is2k = ovi.dwPlatformId == VER_PLATFORM_WIN32_NT && ovi.dwMajorVersion > 4;
    if (AreFileApisANSI()) {
	cp = is2k ? CP_THREAD_ACP : CP_ACP;
    }
    nchar = WideCharToMultiByte(cp, 0, wstr, len, NULL, 0, 0, 0);
    str = xmalloc((nchar + 1) * 2);
    if (!str) {
	return NULL;
    }
    str[0] = '\0';
    nchar = WideCharToMultiByte(cp, 0, wstr, len, str, nchar * 2, 0, 0);
    str[nchar] = '\0';
    return str;
}

#endif /* WINTERFACE */

#endif /* _WIN32 || _WIN64 */
