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

grammar RestrictedRegoExpression;

expression: orExpression EOF;

orExpression: andExpression (OR andExpression)*;

andExpression: notExpression (AND notExpression)*;

notExpression
    : NOT notExpression
    | comparisonExpression
    ;

comparisonExpression: primary (comparisonOperator primary)?;

comparisonOperator
    : EQ
    | NEQ
    | LT
    | LTE
    | GT
    | GTE
    | IN
    ;

primary
    : columnReference
    | sessionUserReference
    | groupMembership
    | literal
    | arrayLiteral
    | LPAREN orExpression RPAREN
    ;

columnReference: COL LPAREN STRING RPAREN;

sessionUserReference: SESSION_USER LPAREN RPAREN;

groupMembership: IS_GROUP_MEMBER LPAREN STRING RPAREN;

literal
    : STRING
    | NUMBER
    | TRUE
    | FALSE
    | NULL
    ;

arrayLiteral: LBRACKET literal (COMMA literal)* RBRACKET;

OR: 'or';
AND: 'and';
NOT: 'not';
IN: 'in';
COL: 'col';
SESSION_USER: 'session_user';
IS_GROUP_MEMBER: 'is_group_member';
TRUE: 'true';
FALSE: 'false';
NULL: 'null';

EQ: '==';
NEQ: '!=';
LTE: '<=';
LT: '<';
GTE: '>=';
GT: '>';
LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
COMMA: ',';

NUMBER: '-'? ('0' | [1-9] [0-9]*) ('.' [0-9]+)?;

STRING: '"' (ESCAPE | ~["\\\u0000-\u001F])* '"';

fragment ESCAPE: '\\' (["\\/bfnrt] | 'u' HEX HEX HEX HEX);
fragment HEX: [0-9a-fA-F];

WS: [ \t\r\n]+ -> skip;
