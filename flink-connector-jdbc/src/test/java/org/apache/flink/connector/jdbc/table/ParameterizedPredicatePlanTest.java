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

import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests ParameterizedPredicate. */
public class ParameterizedPredicatePlanTest extends TableTestBase {
    @Test
    public void testCombine() {
        ParameterizedPredicate thisParameterizedPredicateEquals =
                new ParameterizedPredicate("`type1`");
        ParameterizedPredicate parameterizedPredicatePlaceHolder =
                new ParameterizedPredicate(
                        JdbcFilterPushdownPreparedStatementVisitor.PUSHDOWN_PREDICATE_PLACEHOLDER);
        thisParameterizedPredicateEquals.combine(
                JdbcFilterPushdownPreparedStatementVisitor.OPERATOR_EQUALS,
                parameterizedPredicatePlaceHolder);
        assertThat(
                        thisParameterizedPredicateEquals
                                .getPredicate()
                                .charAt(
                                        thisParameterizedPredicateEquals
                                                .getIndexesOfPredicatePlaceHolders()
                                                .get(0)))
                .isEqualTo(
                        JdbcFilterPushdownPreparedStatementVisitor.PUSHDOWN_PREDICATE_PLACEHOLDER
                                .charAt(0));

        ParameterizedPredicate thatParameterizedPredicateGTE =
                new ParameterizedPredicate("`type2`");
        thatParameterizedPredicateGTE.combine(
                JdbcFilterPushdownPreparedStatementVisitor.OPERATOR_GREATER_THAN_OR_EQUAL,
                parameterizedPredicatePlaceHolder);
        assertThat(
                        thatParameterizedPredicateGTE
                                .getPredicate()
                                .charAt(
                                        thatParameterizedPredicateGTE
                                                .getIndexesOfPredicatePlaceHolders()
                                                .get(0)))
                .isEqualTo(
                        JdbcFilterPushdownPreparedStatementVisitor.PUSHDOWN_PREDICATE_PLACEHOLDER
                                .charAt(0));

        thisParameterizedPredicateEquals.combine(
                JdbcFilterPushdownPreparedStatementVisitor.OPERATOR_OR,
                thatParameterizedPredicateGTE);
        assertThat(thisParameterizedPredicateEquals.getPredicate())
                .isEqualTo("((`type1` = ?) OR (`type2` >= ?))");
        assertThat(thisParameterizedPredicateEquals.getIndexesOfPredicatePlaceHolders().size())
                .isEqualTo(2);
        assertThat(
                        thisParameterizedPredicateEquals
                                .getPredicate()
                                .charAt(
                                        thisParameterizedPredicateEquals
                                                .getIndexesOfPredicatePlaceHolders()
                                                .get(0)))
                .isEqualTo(
                        JdbcFilterPushdownPreparedStatementVisitor.PUSHDOWN_PREDICATE_PLACEHOLDER
                                .charAt(0));
        assertThat(
                        thisParameterizedPredicateEquals
                                .getPredicate()
                                .charAt(
                                        thisParameterizedPredicateEquals
                                                .getIndexesOfPredicatePlaceHolders()
                                                .get(1)))
                .isEqualTo(
                        JdbcFilterPushdownPreparedStatementVisitor.PUSHDOWN_PREDICATE_PLACEHOLDER
                                .charAt(0));
    }

    @Test
    public void testCombineAll() {

        List<ParameterizedPredicate> predicates = new ArrayList<>();
        int count = 0;
        for (String operator : JdbcFilterPushdownPreparedStatementVisitor.UNARY_OPERATORS) {

            ParameterizedPredicate parameterizedPredicate =
                    new ParameterizedPredicate(
                            String.format("(%s %s)", "`type0_" + count + " `", operator));
            predicates.add(parameterizedPredicate);
            count++;
        }
        for (String operator : JdbcFilterPushdownPreparedStatementVisitor.SIMPLE_BINARY_OPERATORS) {
            ParameterizedPredicate parameterizedPredicate =
                    new ParameterizedPredicate("`type1_" + count + " `");
            count++;
            ParameterizedPredicate parameterizedPredicatePlaceHolder =
                    new ParameterizedPredicate(
                            JdbcFilterPushdownPreparedStatementVisitor
                                    .PUSHDOWN_PREDICATE_PLACEHOLDER);
            parameterizedPredicate.combine(operator, parameterizedPredicatePlaceHolder);
            if (parameterizedPredicate.getIndexesOfPredicatePlaceHolders().size() > 0) {
                assertThat(
                                parameterizedPredicate
                                        .getPredicate()
                                        .charAt(
                                                parameterizedPredicate
                                                        .getIndexesOfPredicatePlaceHolders()
                                                        .get(0)))
                        .isEqualTo(
                                JdbcFilterPushdownPreparedStatementVisitor
                                        .PUSHDOWN_PREDICATE_PLACEHOLDER
                                        .charAt(0));
            }
            predicates.add(parameterizedPredicate);
        }
        for (String operator : JdbcFilterPushdownPreparedStatementVisitor.UNARY_OPERATORS) {

            ParameterizedPredicate parameterizedPredicate =
                    new ParameterizedPredicate(
                            String.format("(%s %s)", "`type2_" + count + " `", operator));
            predicates.add(parameterizedPredicate);
            count++;
        }
        // combine the unary binary then unary to some combinations
        ParameterizedPredicate completePredicate = null;
        for (ParameterizedPredicate predicate : predicates) {
            if (completePredicate == null) {
                completePredicate = new ParameterizedPredicate(predicate);
            } else {
                completePredicate.combine(
                        JdbcFilterPushdownPreparedStatementVisitor.OPERATOR_OR, predicate);
                ArrayList<Integer> indexesOfPredicatePlaceHolders =
                        completePredicate.getIndexesOfPredicatePlaceHolders();
                if (!indexesOfPredicatePlaceHolders.isEmpty()) {
                    for (Integer indexOfPredicatePlaceHolder : indexesOfPredicatePlaceHolders) {
                        // check that the placeholder is where we expect
                        assertThat(
                                        completePredicate
                                                .getPredicate()
                                                .charAt(indexOfPredicatePlaceHolder))
                                .isEqualTo(
                                        JdbcFilterPushdownPreparedStatementVisitor
                                                .PUSHDOWN_PREDICATE_PLACEHOLDER
                                                .charAt(0));
                    }
                }
            }
        }
    }
}
