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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Visitor that convert Expression to ParameterizedPredicate. Return Optional.empty() if we cannot
 * push down the filter.
 */
@Experimental
public class JdbcFilterPushdownPreparedStatementVisitor
        extends ExpressionDefaultVisitor<Optional<ParameterizedPredicate>> {

    protected static final String PLACEHOLDER = "?";

    private final Function<String, String> quoteIdentifierFunction;

    public JdbcFilterPushdownPreparedStatementVisitor(
            Function<String, String> quoteIdentifierFunction) {
        this.quoteIdentifierFunction = quoteIdentifierFunction;
    }

    @Override
    public Optional<ParameterizedPredicate> visit(CallExpression call) {
        FilterPushdownFunction filterPushdownFunction =
                FilterPushdownFunction.valueOf(call.getFunctionDefinition());
        if (filterPushdownFunction == null) {
            return Optional.empty();
        }
        return filterPushdownFunction.apply(this, call.getResolvedChildren());
    }

    private Optional<ParameterizedPredicate> renderBinaryOperator(
            String operator, List<ResolvedExpression> allOperands) {
        Optional<ParameterizedPredicate> leftOperandString = allOperands.get(0).accept(this);

        Optional<ParameterizedPredicate> rightOperandString = allOperands.get(1).accept(this);
        return leftOperandString.flatMap(
                left -> rightOperandString.map(right -> left.combine(operator, right)));
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

        ParameterizedPredicate predicate = new ParameterizedPredicate(PLACEHOLDER);
        Function<ValueLiteralExpression, Serializable> f;
        switch (tpe.getTypeRoot()) {
            case CHAR:
                f = f1 -> litExp.getValueAs(String.class).orElse(null);
                break;
            case VARCHAR:
                f = f1 -> litExp.getValueAs(String.class).orElse(null);
                break;
            case BOOLEAN:
                f = f1 -> litExp.getValueAs(Boolean.class).orElse(null);
                break;
            case DECIMAL:
                f = f1 -> litExp.getValueAs(BigDecimal.class).orElse(null);
                break;
            case TINYINT:
                f = f1 -> litExp.getValueAs(Byte.class).orElse(null);
                break;
            case SMALLINT:
                f = f1 -> litExp.getValueAs(Short.class).orElse(null);
                break;
            case INTEGER:
                f = f1 -> litExp.getValueAs(Integer.class).orElse(null);
                break;
            case BIGINT:
                f = f1 -> litExp.getValueAs(Long.class).orElse(null);
                break;
            case FLOAT:
                f = f1 -> litExp.getValueAs(Float.class).orElse(null);
                break;
            case DOUBLE:
                f = f1 -> litExp.getValueAs(Double.class).orElse(null);
                break;
            case DATE:
                f = f1 -> litExp.getValueAs(LocalDate.class).map(Date::valueOf).orElse(null);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                f = f1 -> litExp.getValueAs(java.sql.Time.class).orElse(null);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                f =
                        f1 ->
                                litExp.getValueAs(LocalDateTime.class)
                                        .map(Timestamp::valueOf)
                                        .orElse(null);
                break;
            default:
                return Optional.empty();
        }
        params[0] = f.apply(litExp);
        predicate.setParameters(params);
        return Optional.of(predicate);
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

    /**
     * Enum to map {@code BuiltInFunctionDefinitions} to required info for further push down
     * processing with {@code JdbcFilterPushdownPreparedStatementVisitor}.
     */
    enum FilterPushdownFunction {
        EQUALS(BuiltInFunctionDefinitions.EQUALS, "="),
        LESS_THAN(BuiltInFunctionDefinitions.LESS_THAN, "<"),
        GREATER_THAN(BuiltInFunctionDefinitions.GREATER_THAN, ">"),
        LESS_THAN_OR_EQUAL(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, "<="),
        GREATER_THAN_OR_EQUAL(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, ">="),
        NOT_EQUALS(BuiltInFunctionDefinitions.NOT_EQUALS, "<>"),
        OR(BuiltInFunctionDefinitions.OR, "OR"),
        AND(BuiltInFunctionDefinitions.AND, "AND"),
        LIKE(BuiltInFunctionDefinitions.LIKE, "LIKE"),
        IS_NULL(BuiltInFunctionDefinitions.IS_NULL, "IS NULL", true, true),
        IS_NOT_NULL(BuiltInFunctionDefinitions.IS_NOT_NULL, "IS NOT NULL", true, true);

        private final BuiltInFunctionDefinition function;
        private final String functionSql;
        private final boolean unaryOperation;
        private final boolean operatorOnTheLeft;

        private static final Map<FunctionDefinition, FilterPushdownFunction>
                BUILT_IN_FUNCTION_2_FILTER_FUNCTION_MAP =
                        Arrays.stream(values())
                                .collect(Collectors.toMap(t -> t.function, Function.identity()));

        FilterPushdownFunction(
                BuiltInFunctionDefinition function,
                String functionSql,
                boolean unaryOperation,
                boolean operatorOnTheLeft) {
            this.function = function;
            this.functionSql = functionSql;
            this.unaryOperation = unaryOperation;
            this.operatorOnTheLeft = operatorOnTheLeft;
        }

        FilterPushdownFunction(BuiltInFunctionDefinition function, String functionSql) {
            this(function, functionSql, false, false);
        }

        Optional<ParameterizedPredicate> apply(
                JdbcFilterPushdownPreparedStatementVisitor visitor, List<ResolvedExpression> args) {
            if (unaryOperation) {
                return visitor.renderUnaryOperator(functionSql, args.get(0), operatorOnTheLeft);
            } else {
                return visitor.renderBinaryOperator(functionSql, args);
            }
        }

        static FilterPushdownFunction valueOf(FunctionDefinition builtInFunctionDefinition) {
            return BUILT_IN_FUNCTION_2_FILTER_FUNCTION_MAP.get(builtInFunctionDefinition);
        }

        @VisibleForTesting
        String getFunctionSql() {
            return functionSql;
        }

        @VisibleForTesting
        boolean isUnaryOperation() {
            return unaryOperation;
        }

        @VisibleForTesting
        boolean isOperatorOnTheLeft() {
            return operatorOnTheLeft;
        }
    }
}
