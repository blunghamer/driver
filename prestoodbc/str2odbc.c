#include "str2odbc.h"

/**
 * Return number of month days.
 * @param year
 * @param month 1..12
 * @result number of month days or 0
 */

static int
getmdays(int year, int month)
{
	static const int mdays[] = {
		31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	int mday;

	if (month < 1)
	{
		return 0;
	}
	mday = mdays[(month - 1) % 12];
	if (mday == 28 && year % 4 == 0 &&
		(!(year % 100 == 0) || year % 400 == 0))
	{
		mday++;
	}
	return mday;
}

/**
 * Internal locale neutral strtod function.
 * @param data pointer to string
 * @param endp pointer for ending character
 * @result double value
 */

#if defined(HAVE_LOCALECONV) || defined(_WIN32) || defined(_WIN64)

double ln_strtod(const char *data, char **endp)
{
	struct lconv *lc = 0;
	char buf[128], *p, *end;
	double value;

	lc = localeconv();
	if (lc && lc->decimal_point && lc->decimal_point[0] &&
		lc->decimal_point[0] != '.')
	{
		strncpy(buf, data, sizeof(buf) - 1);
		buf[sizeof(buf) - 1] = '\0';
		p = strchr(buf, '.');
		if (p)
		{
			*p = lc->decimal_point[0];
		}
		p = buf;
	}
	else
	{
		p = (char *)data;
	}
	value = strtod(p, &end);
	end = (char *)data + (end - p);
	if (endp)
	{
		*endp = end;
	}
	return value;
}

#else

#define ln_strtod(A, B) strtod(A, B)

#endif

/**
 * Convert string to ODBC TIME_STRUCT.
 * @param jdconv when true, allow julian day format
 * @param str string to be converted
 * @param ts output TIME_STRUCT
 * @result 0 on success, -1 on error
 *
 * Strings of the format 'HHMMSS' or 'HH:MM:SS'
 * are converted to a TIME_STRUCT.
 *
 * If the string looks like a floating point number,
 * SQLite3's julian day format is assumed.
 */

int str2time(char *str, TIME_STRUCT *ts)
{
	int i, err = 0, ampm = -1;
	double jd;
	char *p, *q;

	ts->hour = ts->minute = ts->second = 0;	
	p = str;
	while (*p && !isdigit(*p))
	{
		++p;
	}
	q = p;
	i = 0;
	while (*q && isdigit(*q))
	{
		++i;
		++q;
	}
	if (i >= 6)
	{
		char buf[4];

		strncpy(buf, p + 0, 2);
		buf[2] = '\0';
		ts->hour = strtol(buf, NULL, 10);
		strncpy(buf, p + 2, 2);
		buf[2] = '\0';
		ts->minute = strtol(buf, NULL, 10);
		strncpy(buf, p + 4, 2);
		buf[2] = '\0';
		ts->second = strtol(buf, NULL, 10);
		goto done;
	}
	i = 0;
	while (i < 3)
	{
		int n;

		q = NULL;
		n = strtol(p, &q, 10);
		if (!q || q == p)
		{
			if (*q == '\0')
			{
				if (i == 0)
				{
					err = 1;
				}
				goto done;
			}
		}
		if (*q == ':' || *q == '\0' || i == 2)
		{
			switch (i)
			{
			case 0:
				ts->hour = n;
				break;
			case 1:
				ts->minute = n;
				break;
			case 2:
				ts->second = n;
				break;
			}
			++i;
			if (*q)
			{
				++q;
			}
		}
		else
		{
			i = 0;
			while (*q && !isdigit(*q))
			{
				++q;
			}
		}
		p = q;
	}
	if (!err)
	{
		while (*p)
		{
			if ((p[0] == 'p' || p[0] == 'P') &&
				(p[1] == 'm' || p[1] == 'M'))
			{
				ampm = 1;
			}
			else if ((p[0] == 'a' || p[0] == 'A') &&
					 (p[1] == 'm' || p[1] == 'M'))
			{
				ampm = 0;
			}
			++p;
		}
		if (ampm > 0)
		{
			if (ts->hour < 12)
			{
				ts->hour += 12;
			}
		}
		else if (ampm == 0)
		{
			if (ts->hour == 12)
			{
				ts->hour = 0;
			}
		}
	}
done:
	/* final check for overflow */
	if (err || ts->hour > 23 || ts->minute > 59 || ts->second > 59)
	{
		return -1;
	}
	return 0;
}

/**
 * Convert string to ODBC TIMESTAMP_STRUCT. 
 * @param str string to be converted
 * @param tss output TIMESTAMP_STRUCT
 * @result 0 on success, -1 on error
 *
 * Strings of the format 'YYYYMMDDhhmmssff' or 'YYYY-MM-DD hh:mm:ss ff'
 * or 'YYYY/MM/DD hh:mm:ss ff' or 'hh:mm:ss ff YYYY-MM-DD' are
 * converted to a TIMESTAMP_STRUCT. The ISO8601 formats
 *    YYYY-MM-DDThh:mm:ss[.f]Z
 *    YYYY-MM-DDThh:mm:ss[.f]shh:mm
 * are also supported. In case a time zone field is present,
 * the resulting TIMESTAMP_STRUCT is expressed in UTC.
 *
 */

int
str2timestamp(char *str, TIMESTAMP_STRUCT *tss)
{
	int i, m, n, err = 0, ampm = -1;
	double jd;
	char *p, *q, in = '\0', sepc = '\0';

	tss->year = tss->month = tss->day = 0;
	tss->hour = tss->minute = tss->second = 0;
	tss->fraction = 0;	
	p = str;
	while (*p && !isdigit(*p))
	{
		++p;
	}
	q = p;
	i = 0;
	while (*q && isdigit(*q))
	{
		++i;
		++q;
	}
	if (i >= 14)
	{
		char buf[16];

		strncpy(buf, p + 0, 4);
		buf[4] = '\0';
		tss->year = strtol(buf, NULL, 10);
		strncpy(buf, p + 4, 2);
		buf[2] = '\0';
		tss->month = strtol(buf, NULL, 10);
		strncpy(buf, p + 6, 2);
		buf[2] = '\0';
		tss->day = strtol(buf, NULL, 10);
		strncpy(buf, p + 8, 2);
		buf[2] = '\0';
		tss->hour = strtol(buf, NULL, 10);
		strncpy(buf, p + 10, 2);
		buf[2] = '\0';
		tss->minute = strtol(buf, NULL, 10);
		strncpy(buf, p + 12, 2);
		buf[2] = '\0';
		tss->second = strtol(buf, NULL, 10);
		if (i > 14)
		{
			m = i - 14;
			strncpy(buf, p + 14, m);
			while (m < 9)
			{
				buf[m] = '0';
				++m;
			}
			buf[m] = '\0';
			tss->fraction = strtol(buf, NULL, 10);
		}
		m = 7;
		goto done;
	}
	m = i = 0;
	while ((m & 7) != 7)
	{
		q = NULL;
		n = strtol(p, &q, 10);
		if (!q || q == p)
		{
			if (*q == '\0')
			{
				if (m < 1)
				{
					err = 1;
				}
				goto done;
			}
		}
		if (in == '\0')
		{
			switch (*q)
			{
			case '-':
			case '/':
				if ((m & 1) == 0)
				{
					in = *q;
					i = 0;
				}
				break;
			case ':':
				if ((m & 2) == 0)
				{
					in = *q;
					i = 0;
				}
				break;
			case ' ':
			case '.':
				break;
			default:
				in = '\0';
				i = 0;
				break;
			}
		}
		switch (in)
		{
		case '-':
		case '/':
			if (!sepc)
			{
				sepc = in;
			}
			switch (i)
			{
			case 0:
				tss->year = n;
				break;
			case 1:
				tss->month = n;
				break;
			case 2:
				tss->day = n;
				break;
			}
			if (++i >= 3)
			{
				i = 0;
				m |= 1;
				if (!(m & 2))
				{
					m |= 8;
				}
				goto skip;
			}
			else
			{
				++q;
			}
			break;
		case ':':
			switch (i)
			{
			case 0:
				tss->hour = n;
				break;
			case 1:
				tss->minute = n;
				break;
			case 2:
				tss->second = n;
				break;
			}
			if (++i >= 3)
			{
				i = 0;
				m |= 2;
				if (*q == '.')
				{
					in = '.';
					goto skip2;
				}
				if (*q == ' ')
				{
					if ((m & 1) == 0)
					{
						char *e = NULL;

						(void)strtol(q + 1, &e, 10);
						if (e && *e == '-')
						{
							goto skip;
						}
					}
					in = '.';
					goto skip2;
				}
				goto skip;
			}
			else
			{
				++q;
			}
			break;
		case '.':
			if (++i >= 1)
			{
				int ndig = q - p;

				if (p[0] == '+' || p[0] == '-')
				{
					ndig--;
				}
				while (ndig < 9)
				{
					n = n * 10;
					++ndig;
				}
				tss->fraction = n;
				m |= 4;
				i = 0;
			}
		default:
		skip:
			in = '\0';
		skip2:
			while (*q && !isdigit(*q))
			{
				if ((q[0] == 'a' || q[0] == 'A') &&
					(q[1] == 'm' || q[1] == 'M'))
				{
					ampm = 0;
					++q;
				}
				else if ((q[0] == 'p' || q[0] == 'P') &&
						 (q[1] == 'm' || q[1] == 'M'))
				{
					ampm = 1;
					++q;
				}
				++q;
			}
		}
		p = q;
	}
	if ((m & 7) > 1 && (m & 8))
	{
		/* ISO8601 timezone */
		if (p > str && isdigit(*p))
		{
			int nn, sign;

			q = p - 1;
			if (*q != '+' && *q != '-')
			{
				goto done;
			}
			sign = (*q == '+') ? -1 : 1;
			q = NULL;
			n = strtol(p, &q, 10);
			if (!q || *q++ != ':' || !isdigit(*q))
			{
				goto done;
			}
			p = q;
			q = NULL;
			nn = strtol(p, &q, 10);
			tss->minute += nn * sign;
			if ((SQLSMALLINT)tss->minute < 0)
			{
				tss->hour -= 1;
				tss->minute += 60;
			}
			else if (tss->minute >= 60)
			{
				tss->hour += 1;
				tss->minute -= 60;
			}
			tss->hour += n * sign;
			if ((SQLSMALLINT)tss->hour < 0)
			{
				tss->day -= 1;
				tss->hour += 24;
			}
			else if (tss->hour >= 24)
			{
				tss->day += 1;
				tss->hour -= 24;
			}
			if ((short)tss->day < 1 || tss->day >= 28)
			{
				int mday, pday, pmon;

				mday = getmdays(tss->year, tss->month);
				pmon = tss->month - 1;
				if (pmon < 1)
				{
					pmon = 12;
				}
				pday = getmdays(tss->year, pmon);
				if ((SQLSMALLINT)tss->day < 1)
				{
					tss->month -= 1;
					tss->day = pday;
				}
				else if (tss->day > mday)
				{
					tss->month += 1;
					tss->day = 1;
				}
				if ((SQLSMALLINT)tss->month < 1)
				{
					tss->year -= 1;
					tss->month = 12;
				}
				else if (tss->month > 12)
				{
					tss->year += 1;
					tss->month = 1;
				}
			}
		}
	}
done:
	if ((m & 1) &&
		(tss->month < 1 || tss->month > 12 ||
		 tss->day < 1 || tss->day > getmdays(tss->year, tss->month)))
	{
		if (sepc == '/')
		{
			/* Try MM/DD/YYYY format */
			int t[3];

			t[0] = tss->year;
			t[1] = tss->month;
			t[2] = tss->day;
			tss->year = t[2];
			tss->day = t[1];
			tss->month = t[0];
		}
	}
	/* Replace missing year/month/day with current date */
	if (!err && (m & 1) == 0)
	{
#ifdef _WIN32
		SYSTEMTIME t;

		GetLocalTime(&t);
		tss->year = t.wYear;
		tss->month = t.wMonth;
		tss->day = t.wDay;
#else
		struct timeval tv;
		struct tm tm;

		gettimeofday(&tv, NULL);
		tm = *localtime(&tv.tv_sec);
		tss->year = tm.tm_year + 1900;
		tss->month = tm.tm_mon + 1;
		tss->day = tm.tm_mday;
#endif
	}
	/* Normalize fraction */
	if (tss->fraction < 0)
	{
		tss->fraction = 0;
	}
	/* Final check for overflow */
	if (err ||
		tss->month < 1 || tss->month > 12 ||
		tss->day < 1 || tss->day > getmdays(tss->year, tss->month) ||
		tss->hour > 23 || tss->minute > 59 || tss->second > 59)
	{
		return -1;
	}
	if ((m & 7) > 1)
	{
		if (ampm > 0)
		{
			if (tss->hour < 12)
			{
				tss->hour += 12;
			}
		}
		else if (ampm == 0)
		{
			if (tss->hour == 12)
			{
				tss->hour = 0;
			}
		}
	}
	return ((m & 7) < 1) ? -1 : 0;
}


/**
 * Convert string to ODBC DATE_STRUCT.
 * @param str string to be converted
 * @param ds output DATE_STRUCT
 * @result 0 on success, -1 on error
 *
 * Strings of the format 'YYYYMMDD' or 'YYYY-MM-DD' or
 * 'YYYY/MM/DD' or 'MM/DD/YYYY' are converted to a
 * DATE_STRUCT.
 *
 * If the string looks like a floating point number,
 * SQLite3's julian day format is assumed.
 */

int
str2date(char *str, DATE_STRUCT *ds)
{
	int i, err = 0;
	double jd;
	char *p, *q, sepc = '\0';

	ds->year = ds->month = ds->day = 0;	
	p = str;
	while (*p && !isdigit(*p))
	{
		++p;
	}
	q = p;
	i = 0;
	while (*q && !isdigit(*q))
	{
		++i;
		++q;
	}
	if (i >= 8)
	{
		char buf[8];

		strncpy(buf, p + 0, 4);
		buf[4] = '\0';
		ds->year = strtol(buf, NULL, 10);
		strncpy(buf, p + 4, 2);
		buf[2] = '\0';
		ds->month = strtol(buf, NULL, 10);
		strncpy(buf, p + 6, 2);
		buf[2] = '\0';
		ds->day = strtol(buf, NULL, 10);
		goto done;
	}
	i = 0;
	while (i < 3)
	{
		int n;

		q = NULL;
		n = strtol(p, &q, 10);
		if (!q || q == p)
		{
			if (*q == '\0')
			{
				if (i == 0)
				{
					err = 1;
				}
				goto done;
			}
		}
		if (!sepc)
		{
			sepc = *q;
		}
		if (*q == '-' || *q == '/' || *q == '\0' || i == 2)
		{
			switch (i)
			{
			case 0:
				ds->year = n;
				break;
			case 1:
				ds->month = n;
				break;
			case 2:
				ds->day = n;
				break;
			}
			++i;
			if (*q)
			{
				++q;
			}
		}
		else
		{
			i = 0;
			while (*q && !isdigit(*q))
			{
				++q;
			}
		}
		p = q;
	}
done:
	/* final check for overflow */
	if (err ||
		ds->month < 1 || ds->month > 12 ||
		ds->day < 1 || ds->day > getmdays(ds->year, ds->month))
	{
		if (sepc == '/')
		{
			/* Try MM/DD/YYYY format */
			int t[3];

			t[0] = ds->year;
			t[1] = ds->month;
			t[2] = ds->day;
			ds->year = t[2];
			ds->day = t[1];
			ds->month = t[0];
			if (ds->month >= 1 && ds->month <= 12 &&
				(ds->day >= 1 || ds->day <= getmdays(ds->year, ds->month)))
			{
				return 0;
			}
		}
		return -1;
	}
	return 0;
}