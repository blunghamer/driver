
#ifndef __JSONPARSER_HH
#define __JSONPARSER_HH

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include "prestoclienttypes.h"

enum E_JSON_READSTATES
{
	JSON_RS_SEARCH_OBJECT = 0
,	JSON_RS_READ_STRING
,	JSON_RS_READ_NONSTRING
};

enum E_JSON_CONTROL_CHARS
{
	JSON_CC_NONE = 0
,	JSON_CC_WS			// Whitespace (space, tab, line feed, form feed, carriage return)
,	JSON_CC_OO			// Object open {
,	JSON_CC_OC			// Object close }
,	JSON_CC_AO			// Array open [
,	JSON_CC_AC			// Array close ]
,	JSON_CC_BS			// Backslash
,	JSON_CC_QT			// Double quote
,	JSON_CC_COLON		// Colon
,	JSON_CC_COMMA		// Comma
};

enum E_JSON_TAGTYPES
{
	JSON_TT_UNKNOWN = 0
,	JSON_TT_STRING
,	JSON_TT_NUMBER
,	JSON_TT_OBJECT_OPEN
,	JSON_TT_OBJECT_CLOSE
,	JSON_TT_ARRAY_OPEN
,	JSON_TT_ARRAY_CLOSE
,	JSON_TT_COLON
,	JSON_TT_COMMA
,	JSON_TT_TRUE
,	JSON_TT_FALSE
,	JSON_TT_NULL
};


/* --- Structs -------------------------------------------------------------------------------------------------------- */
typedef struct ST_JSONPARSER
{
	enum E_JSON_READSTATES		  state;						//!< State of state-machine
	bool						  isbackslash;					//!< If true, the previous character was a BS
	unsigned int				  readposition;					//!< Readposition within curl buffer
	bool						  skipnextread;					//!< If true don't read the next character, keep the current character
	bool						  error;						//!< Set to true when a parse error is detected
	char						 *c;							//!< Current character
	enum E_JSON_CONTROL_CHARS	  control;						//!< Meaning of current character as control character
	unsigned int				  clength;						//!< Length of current character (1..4 bytes)
	char						 *tagbuffer;					//!< Buffer for storing tag that is currently being read
	unsigned int				  tagbuffersize;				//!< Maximum size of tag buffer
	unsigned int				  tagbufferactualsize;			//!< Actual size of tag buffer
	enum E_JSON_TAGTYPES		  tagtype;						//!< Type of value returned by the parser
} JSONPARSER;

typedef struct ST_JSONLEXER
{
	enum E_JSON_TAGTYPES		  previoustag;					//!< json type of the previous tag
	enum E_JSON_TAGTYPES		  previoustag1;					//!< json type of the previous tag
	enum E_JSON_TAGTYPES		  previoustag2;					//!< json type of the previous tag
	enum E_JSON_TAGTYPES		  previoustag3;					//!< json type of the previous tag
	enum E_JSON_TAGTYPES		  previoustag4;					//!< json type of the previous tag
	enum E_JSON_TAGTYPES		 *tagorder;						//!< Array containing types of json parent elements of the current tag
	char						**tagordername;					//!< Array containing name of json parent elements
	unsigned int				  tagordersize;					//!< Maximum number of elements for tagorder array
	unsigned int				  tagorderactualsize;			//!< Actual number of elements used in tagorder array
	unsigned int				  column;						//!< Column index of current tag
	bool						  error;						//!< Set to true when a lexer error is detected
	char						 *name;							//!< Last found name string
	unsigned int				  namesize;						//!< Maximum length of name
	unsigned int				  nameactualsize;				//!< Actual length of name
	char						 *value;						//!< Last found value string
	unsigned int				  valuesize;					//!< Maximum length of value
	unsigned int				  valueactualsize;				//!< Actual length of value

} JSONLEXER;

extern void json_delete_parser(JSONPARSER* json);
extern void json_delete_lexer(JSONLEXER* lexer);
extern void json_reset_lexer(JSONLEXER* lexer);

#endif


