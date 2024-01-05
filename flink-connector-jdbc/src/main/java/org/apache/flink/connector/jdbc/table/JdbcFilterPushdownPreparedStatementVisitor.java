/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Visitor that convert Expression to ParameterizedPredicate. Return Optional.empty() if we cannot
 * push down the filter.
 */
@Experimental
public class JdbcFilterPushdownPreparedStatementVisitor
        extends ExpressionDefaultVisitor<Optional<ParameterizedPredicate>> {

    protected static final String PUSHDOWN_PREDICATE_PLACEHOLDER = "?";

    protected static final String OPERATOR_EQUALS = "=";

    protected static final String OPERATOR_LESS_THAN = "<";

    protected static final String OPERATOR_LESS_THAN_OR_EQUAL = "<=";

    protected static final String OPERATOR_GREATER_THAN = ">";

    protected static final String OPERATOR_GREATER_THAN_OR_EQUAL = ">=";

    protected static final String OPERATOR_NOT_EQUALS = "<>";

    protected static final String OPERATOR_OR = "OR";

    protected static final String OPERATOR_AND = "AND";

    protected static final String OPERATOR_LIKE = "LIKE";

    protected static final String OPERATOR_IS_NULL = "IS NULL";

    protected static final String OPERATOR_IS_NOT_NULL = "IS NOT NULL";

    private static String[] simpleBinaryOperatorValues =
            new String[] {
                OPERATOR_EQUALS,
                OPERATOR_LESS_THAN,
                OPERATOR_LESS_THAN_OR_EQUAL,
                OPERATOR_GREATER_THAN,
                OPERATOR_GREATER_THAN_OR_EQUAL,
                OPERATOR_NOT_EQUALS,
                OPERATOR_LIKE
            };
    private static String[] unaryOperatorValues =
            new String[] {OPERATOR_IS_NULL, OPERATOR_IS_NOT_NULL};

    private final Function<String, String> quoteIdentifierFunction;

    @VisibleForTesting
    protected static final Set<String> SIMPLE_BINARY_OPERATORS =
            new HashSet<>(Arrays.asList(simpleBinaryOperatorValues));

    @VisibleForTesting
    protected static final Set<String> UNARY_OPERATORS =
            new HashSet<>(Arrays.asList(unaryOperatorValues));

    public JdbcFilterPushdownPreparedStatementVisitor(
            Function<String, String> quoteIdentifierFunction) {
        this.quoteIdentifierFunction = quoteIdentifierFunction;
    }

    @Override
    public Optional<ParameterizedPredicate> visit(CallExpression call) {
        if (BuiltInFunctionDefinitions.EQUALS.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_EQUALS, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_LESS_THAN, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_LESS_THAN_OR_EQUAL, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_GREATER_THAN, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_GREATER_THAN_OR_EQUAL, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_NOT_EQUALS, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.OR.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_OR, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.AND.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_AND, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LIKE.equals(call.getFunctionDefinition())) {
            return renderBinaryOperator(OPERATOR_LIKE, call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.IS_NULL.equals(call.getFunctionDefinition())) {
            return renderUnaryOperator(OPERATOR_IS_NULL, call.getResolvedChildren().get(0), true);
        }
        if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(call.getFunctionDefinition())) {
            return renderUnaryOperator(
                    OPERATOR_IS_NOT_NULL, call.getResolvedChildren().get(0), true);
        }

        return Optional.empty();
    }

    private Optional<ParameterizedPredicate> renderBinaryOperator(
            String operator, List<ResolvedExpression> allOperands) {
        Optional<ParameterizedPredicate> leftOperandString = allOperands.get(0).accept(this);

        Optional<ParameterizedPredicate> rightOperandString = allOperands.get(1).accept(this);
        Optional<ParameterizedPredicate> renderedParameterizedPredicate =
                leftOperandString.flatMap(
                        left -> rightOperandString.map(right -> left.combine(operator, right)));
        return renderedParameterizedPredicate;
    }

    @VisibleForTesting
    protected Optional<ParameterizedPredicate> renderUnaryOperator(
            String operator, ResolvedExpression operand, boolean operandOnLeft) {
        if (operand instanceof FieldReferenceExpression) {
            Optional<ParameterizedPredicate> fieldPartialPredicate =
                    this.visit((FieldReferenceExpression) operand);
            if (operandOnLeft) {
                return fieldPartialPredicate.map(
                        fieldPred ->
                                new ParameterizedPredicate(
                                        String.format(
                                                "(%s %s)", fieldPred.getPredicate(), operator)));
            } else {
                return fieldPartialPredicate.map(
                        fieldPred ->
                                new ParameterizedPredicate(
                                        String.format(
                                                "(%s %s)", operator, fieldPred.getPredicate())));
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ParameterizedPredicate> visit(ValueLiteralExpression litExp) {
        LogicalType tpe = litExp.getOutputDataType().getLogicalType();
        Serializable[] params = new Serializable[1];

        ParameterizedPredicate predicate =
                new ParameterizedPredicate(PUSHDOWN_PREDICATE_PLACEHOLDER);

        switch (tpe.getTypeRoot()) {
            case CHAR:
                params[0] = litExp.getValueAs(String.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case VARCHAR:
                params[0] = litExp.getValueAs(String.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case BOOLEAN:
                params[0] = litExp.getValueAs(Boolean.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case DECIMAL:
                params[0] = litExp.getValueAs(BigDecimal.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case TINYINT:
                params[0] = litExp.getValueAs(Byte.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case SMALLINT:
                params[0] = litExp.getValueAs(Short.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case INTEGER:
                params[0] = litExp.getValueAs(Integer.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case BIGINT:
                params[0] = litExp.getValueAs(Long.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case FLOAT:
                params[0] = litExp.getValueAs(Float.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case DOUBLE:
                params[0] = litExp.getValueAs(Double.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case DATE:
                params[0] = litExp.getValueAs(LocalDate.class).map(Date::valueOf).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case TIME_WITHOUT_TIME_ZONE:
                params[0] = litExp.getValueAs(java.sql.Time.class).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                params[0] =
                        litExp.getValueAs(LocalDateTime.class).map(Timestamp::valueOf).orElse(null);
                predicate.setParameters(params);
                return Optional.of(predicate);
            default:
                return Optional.empty();
        }
    }

    @Override
    public Optional<ParameterizedPredicate> visit(FieldReferenceExpression fieldReference) {
        String predicateStr = this.quoteIdentifierFunction.apply(fieldReference.toString());
        ParameterizedPredicate predicate = new ParameterizedPredicate(predicateStr);
        return Optional.of(predicate);
    }

    @Override
    protected Optional<ParameterizedPredicate> defaultMethod(Expression expression) {
        return Optional.empty();
    }
}
