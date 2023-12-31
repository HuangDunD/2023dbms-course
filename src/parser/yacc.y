%{
#include "ast.h"
#include "yacc.tab.h"
#include <iostream>
#include <memory>

int yylex(YYSTYPE *yylval, YYLTYPE *yylloc);

void yyerror(YYLTYPE *locp, const char* s) {
    std::cerr << "Parser Error at line " << locp->first_line << " column " << locp->first_column << ": " << s << std::endl;
}

using namespace ast;
%}

// request a pure (reentrant) parser
%define api.pure full
// enable location in error handler
%locations
// enable verbose syntax error message
%define parse.error verbose

// keywords
%token SHOW TABLES CREATE TABLE DROP DESC INSERT INTO VALUES DELETE FROM ASC
WHERE UPDATE SET SELECT INT BIGINT CHAR DATETIME FLOAT INDEX AND JOIN EXIT HELP TXN_BEGIN TXN_COMMIT TXN_ABORT TXN_ROLLBACK ORDER_BY
SUM COUNT MAX MIN AS LIMIT ON OFF LOAD
// non-keywords
%token LEQ NEQ GEQ T_EOF

// type-specific tokens
%token <sv_str> IDENTIFIER VALUE_STRING PATH
%token <sv_int> VALUE_INT
%token <sv_float> VALUE_FLOAT

// specify types for non-terminal symbol
%type <sv_node> stmt dbStmt ddl dml txnStmt
%type <sv_field> field
%type <sv_fields> fieldList
%type <sv_type_len> type
%type <sv_comp_op> op
%type <sv_agg_type> AGGREGATE
%type <sv_expr> expr
%type <sv_val> value optLIMITClause
%type <sv_vals> valueList
%type <sv_str> tbName colName aliasName optAsClause setValue
%type <sv_strs> tableList colNameList
%type <sv_col> col
%type <sv_cols> colList selector
%type <sv_set_clause> setClause
%type <sv_set_clauses> setClauses
%type <sv_cond> condition
%type <sv_conds> whereClause optWhereClause
%type <sv_orderby>  order_clause opt_order_clause
%type <sv_orderby_dir> opt_asc_desc


%%
start:
        stmt ';'
    {
        parse_tree = $1;
        YYACCEPT;
    }
    |   SET colName setValue
    {
        parse_tree = std::make_shared<SetParam>($2, $3);
        YYACCEPT;
    }
    |   HELP
    {
        parse_tree = std::make_shared<Help>();
        YYACCEPT;
    }
    |   EXIT
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    |   T_EOF
    {
        parse_tree = nullptr;
        YYACCEPT;
    }
    ;

stmt:
        dbStmt
    |   ddl
    |   dml
    |   txnStmt
    ;

txnStmt:
        TXN_BEGIN
    {
        $$ = std::make_shared<TxnBegin>();
    }
    |   TXN_COMMIT
    {
        $$ = std::make_shared<TxnCommit>();
    }
    |   TXN_ABORT
    {
        $$ = std::make_shared<TxnAbort>();
    }
    | TXN_ROLLBACK
    {
        $$ = std::make_shared<TxnRollback>();
    }
    ;

dbStmt:
        SHOW TABLES
    {
        $$ = std::make_shared<ShowTables>();
    }
    |   SHOW INDEX FROM tbName
    {
        $$ = std::make_shared<ShowIndex>($4);
    }
    ;

ddl:
        CREATE TABLE tbName '(' fieldList ')'
    {
        $$ = std::make_shared<CreateTable>($3, $5);
    }
    |   DROP TABLE tbName
    {
        $$ = std::make_shared<DropTable>($3);
    }
    |   DESC tbName
    {
        $$ = std::make_shared<DescTable>($2);
    }
    |   CREATE INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<CreateIndex>($3, $5);
    }
    |   DROP INDEX tbName '(' colNameList ')'
    {
        $$ = std::make_shared<DropIndex>($3, $5);
    }
    ;

dml:
        INSERT INTO tbName VALUES '(' valueList ')'
    {
        $$ = std::make_shared<InsertStmt>($3, $6);
    }
    |   DELETE FROM tbName optWhereClause
    {
        $$ = std::make_shared<DeleteStmt>($3, $4);
    }
    |   UPDATE tbName SET setClauses optWhereClause
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $5);
    }
    |   SELECT selector FROM tableList optWhereClause opt_order_clause optLIMITClause
    {
        $$ = std::make_shared<SelectStmt>($2, $4, $5, $6, $7);
    }
    |   LOAD PATH INTO tbName
    {
        $$ = std::make_shared<LoadStmt>($2, $4);
    }
    ;

selector:
        '*'
    {
        $$ = {};
    }
    |   colList
    ;

colList:
        col
    {
        $$ = std::vector<std::shared_ptr<Col>>{$1};
    }
    |   colList ',' col
    {
        $$.push_back($3);
    }
    ;

col:
        AGGREGATE '(' tbName '.' colName ')' optAsClause
    {
        $$ = std::make_shared<Col>($3, $5, $1, $7);
    }
    |
        AGGREGATE '(' colName ')' optAsClause
    {
        $$ = std::make_shared<Col>("", $3, $1, $5);
    }
    |    
        AGGREGATE '(' '*' ')' optAsClause
    {
        $$ = std::make_shared<Col>("", "", $1, $5);
    }
    |
        tbName '.' colName optAsClause
    {
        $$ = std::make_shared<Col>($1, $3, SV_AGG_NONE, $4);
    }
    |   colName optAsClause
    {
        $$ = std::make_shared<Col>("", $1, SV_AGG_NONE, $2);
    }
    ;

AGGREGATE:
        SUM
    {
        $$ = SV_AGG_SUM;
    }
    |   COUNT
    {
        $$ = SV_AGG_COUNT;
    }
    |   MAX
    {
        $$ = SV_AGG_MAX;
    }
    |   MIN
    {
        $$ = SV_AGG_MIN;
    }
    ;

optAsClause:
        /* epsilon */ { /* ignore*/ }
    |   AS aliasName
    {
        $$ = $2;
    }
    ;

fieldList:
        field
    {
        $$ = std::vector<std::shared_ptr<Field>>{$1};
    }
    |   fieldList ',' field
    {
        $$.push_back($3);
    }
    ;

setValue:
        ON
    {
        $$ = "1";
    }
    |   OFF
    {
        $$ = "0";
    }
    ;

colNameList:
        colName
    {
        $$ = std::vector<std::string>{$1};
    }
    | colNameList ',' colName
    {
        $$.push_back($3);
    }
    ;

field:
        colName type
    {
        $$ = std::make_shared<ColDef>($1, $2);
    }
    ;

type:
        INT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_INT, sizeof(int));
    }
    |    BIGINT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_BIGINT, sizeof(long long));
    }
    |   CHAR '(' VALUE_INT ')'
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_STRING, $3);
    }
    |   DATETIME
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_DATETIME, 19);
    }
    |   FLOAT
    {
        $$ = std::make_shared<TypeLen>(SV_TYPE_FLOAT, sizeof(float));
    }
    ;

valueList:
        value
    {
        $$ = std::vector<std::shared_ptr<Value>>{$1};
    }
    |   valueList ',' value
    {
        $$.push_back($3);
    }
    ;

value:
        VALUE_INT
    {
        $$ = std::make_shared<BigIntLit>($1);
    }
    |   VALUE_FLOAT
    {
        $$ = std::make_shared<FloatLit>($1);
    }
    |   VALUE_STRING
    {
        $$ = std::make_shared<StringLit>($1);
    }
    ;

condition:
        col op expr
    {
        $$ = std::make_shared<BinaryExpr>($1, $2, $3);
    }
    ;

/* 就依葫芦画瓢 */
optLIMITClause:
        { /* ignore*/ }
    |   LIMIT value
    {
        $$ = std::static_pointer_cast<Value>($2);
    }
    ;

optWhereClause:
        /* epsilon */ { /* ignore*/ }
    |   WHERE whereClause
    {
        $$ = $2;
    }
    ;

whereClause:
        condition 
    {
        $$ = std::vector<std::shared_ptr<BinaryExpr>>{$1};
    }
    |   whereClause AND condition
    {
        $$.push_back($3);
    }
    ;

op:
        '='
    {
        $$ = SV_OP_EQ;
    }
    |   '<'
    {
        $$ = SV_OP_LT;
    }
    |   '>'
    {
        $$ = SV_OP_GT;
    }
    |   NEQ
    {
        $$ = SV_OP_NE;
    }
    |   LEQ
    {
        $$ = SV_OP_LE;
    }
    |   GEQ
    {
        $$ = SV_OP_GE;
    }
    ;

expr:
        value
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    |   col
    {
        $$ = std::static_pointer_cast<Expr>($1);
    }
    ;

setClauses:
        setClause
    {
        $$ = std::vector<std::shared_ptr<SetClause>>{$1};
    }
    |   setClauses ',' setClause
    {
        $$.push_back($3);
    }
    ;

setClause:
        colName '=' value
    {
        $$ = std::make_shared<SetClause>($1, $3);
    }
    |  colName '=' colName value
    {
        $4->reverse();
        $$ = std::make_shared<SetClause>($1, $4, $3);
    } 
    ;

tableList:
        tbName
    {
        $$ = std::vector<std::string>{$1};
    }
    |   tableList ',' tbName
    {
        $$.push_back($3);
    }
    |   tableList JOIN tbName
    {
        $$.push_back($3);
    }
    ;

opt_order_clause:
    ORDER_BY order_clause
    { 
        $$ = $2;
        // $$ = std::make_shared<OrderBy>($3,$4);
    }
    |   /* epsilon */ { /* ignore*/ }
    ;

order_clause:
        col opt_asc_desc
    {
        $$ = std::make_shared<OrderBy>($1,$2);
        // $$->cols.insert($$->cols.end(), $4->cols.begin(), $4->cols.end());
        // $$->orderby_dir.insert($$->orderby_dir.end(), $4->orderby_dir.begin(), $4->orderby_dir.end());
    }
    |  order_clause ',' col opt_asc_desc
    {  
        $$->cols.push_back($3);
        $$->orderby_dir.push_back($4);
    }
    ;   

opt_asc_desc:
    ASC          { $$ = OrderBy_ASC;     }
    |  DESC      { $$ = OrderBy_DESC;    }
    |       { $$ = OrderBy_DEFAULT; }
    ;    

tbName: IDENTIFIER;

colName: IDENTIFIER;

aliasName: IDENTIFIER;
%%
