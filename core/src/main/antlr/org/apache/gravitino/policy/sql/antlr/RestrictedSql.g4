/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
grammar RestrictedSql;

options { caseInsensitive = true; }

definition: LPAREN (parameter (COMMA parameter)*)? RPAREN RETURNS dataType ARROW expression EOF;
parameter: identifier dataType;
dataType
    : BOOLEAN | INTEGER | LONG | FLOAT | DOUBLE | STRING_TYPE | BINARY | DATE
    | DECIMAL LPAREN NUMBER COMMA NUMBER RPAREN
    | TIME (LPAREN NUMBER RPAREN)?
    | TIMESTAMP (LPAREN NUMBER RPAREN)? ((WITH | WITHOUT) TIME ZONE)?
    ;
expression: orExpression;
orExpression: andExpression (OR andExpression)*;
andExpression: notExpression (AND notExpression)*;
notExpression: NOT* predicate;
predicate
    : scalar (comparisonOperator scalar
        | IS NOT? NULL
        | NOT? IN LPAREN literal (COMMA literal)* RPAREN
        | NOT? BETWEEN scalar AND scalar
        | NOT? LIKE STRING)?
    ;
comparisonOperator: EQ | NEQ | LT | LTE | GT | GTE;
scalar
    : identifier
    | literal
    | functionCall
    | caseExpression
    | LPAREN expression RPAREN
    ;
functionCall: identifier LPAREN (expression (COMMA expression)*)? RPAREN;
caseExpression: CASE expression? whenClause+ (ELSE expression)? END;
whenClause: WHEN expression THEN expression;
literal
    : STRING | NUMBER | TRUE | FALSE | NULL
    | DATE STRING
    | TIME STRING
    | TIMESTAMP (WITH TIME ZONE)? STRING
    ;
identifier: IDENTIFIER | QUOTED_IDENTIFIER;

RETURNS: 'RETURNS';
BOOLEAN: 'BOOLEAN';
INTEGER: 'INTEGER';
LONG: 'LONG';
FLOAT: 'FLOAT';
DOUBLE: 'DOUBLE';
STRING_TYPE: 'STRING';
BINARY: 'BINARY';
DATE: 'DATE';
DECIMAL: 'DECIMAL';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
WITH: 'WITH';
WITHOUT: 'WITHOUT';
ZONE: 'ZONE';
OR: 'OR';
AND: 'AND';
NOT: 'NOT';
IS: 'IS';
NULL: 'NULL';
IN: 'IN';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
TRUE: 'TRUE';
FALSE: 'FALSE';
ARROW: '->';
LPAREN: '(';
RPAREN: ')';
COMMA: ',';
EQ: '=';
NEQ: '<>' | '!=';
LTE: '<=';
GTE: '>=';
LT: '<';
GT: '>';
NUMBER: '-'? ('0' | [1-9] [0-9]*) ('.' [0-9]+)?;
STRING: '\'' ('\'\'' | ~'\'')* '\'';
QUOTED_IDENTIFIER: '"' ('""' | ~'"')* '"';
IDENTIFIER: [a-z_] [a-z_0-9]*;
WHITESPACE: [ \t\r\n]+ -> skip;
