    /* keywords are case insensitive */
%option caseless
    /* we don't need yywrap() function */
%option noyywrap
    /* we don't need yyunput() function */
%option nounput
    /* we don't need input() function */
%option noinput
    /* enable location */
%option bison-bridge
%option bison-locations

%{
#include "ast.h"
#include "errors.h"
#include "yacc.tab.h"
#include <iostream>
#include <errno.h>

// automatically update location
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for (int i = 0; yytext[i] != '\0'; i++) { \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 1; \
        } else { \
            yylloc->last_column++; \
        } \
    }

%}

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?\ ?{digit}+\.({digit}+)?
value_string '[^']*'
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."
path \.\.[a-zA-Z0-9_\/\.\-]+

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { /* ignore new line */ }
    /* keywords */
"SHOW" { return SHOW; }
"BEGIN" { return TXN_BEGIN; }
"COMMIT" { return TXN_COMMIT; }
"ABORT" { return TXN_ABORT; }
"ROLLBACK" { return TXN_ROLLBACK; }
"TABLES" { return TABLES; }
"CREATE" { return CREATE; }
"TABLE" { return TABLE; }
"DROP" { return DROP; }
"DESC" { return DESC; }
"INSERT" { return INSERT; }
"INTO" { return INTO; }
"VALUES" { return VALUES; }
"DELETE" { return DELETE; }
"FROM" { return FROM; }
"WHERE" { return WHERE; }
"UPDATE" { return UPDATE; }
"SET" { return SET; }
"ON" { return ON; }
"OFF" { return OFF; }
"SELECT" { return SELECT; }
"INT" { return INT; }
"BIGINT" { return BIGINT; }
"CHAR" { return CHAR; }
"DATETIME" { return DATETIME; }
"FLOAT" { return FLOAT; }
"INDEX" { return INDEX; }
"AND" { return AND; }
"JOIN" {return JOIN;}
"LOAD" { return LOAD; }
"EXIT" { return EXIT; }
"HELP" { return HELP; }
"ORDER BY" {  return ORDER_BY;  }
"ASC" { return ASC; }
"LIMIT" { return LIMIT; }
"SUM" { return SUM; }
"COUNT" {return COUNT; }
"MAX" { return MAX; }
"MIN" {return MIN; }
"AS" { return AS; }
    /* operators */
">=" { return GEQ; }
"<=" { return LEQ; }
"<>" { return NEQ; }
{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->sv_str = yytext;
    return IDENTIFIER;
}
    /* literals */
{value_int} {
    // yylval->sv_int = atoi(yytext);
    errno = 0;
    yylval->sv_int = strtoll(yytext, NULL, 10);
    if (errno) {
        printf("errON[%d]errMsg[%s]\n",errno,strerror(errno));
        throw ResultOutOfRangeError();
        return -1;
    }
    return VALUE_INT;
}
{value_float} {
    // 去除yytext中的空格
    char *p = yytext;
    char *q = yytext;
    do {
        if(*p != ' ') {
            *q++ = *p;
        }
    } while(*p++ != '\0');
    yylval->sv_float = atof(yytext);
    return VALUE_FLOAT;
}
{value_string} {
    yylval->sv_str = std::string(yytext + 1, strlen(yytext) - 2);
    return VALUE_STRING;
}
{path} {
    yylval->sv_str = yytext;
    return PATH;
}
    /* EOF */
<<EOF>> { return T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%
