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

import org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction.AND;
import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction.EQUALS;
import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction.GREATER_THAN;
import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction.GREATER_THAN_OR_EQUAL;
import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction.IS_NOT_NULL;
import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.FilterPushdownFunction.OR;
import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.PLACEHOLDER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests ParameterizedPredicate. */
class ParameterizedPredicatePlanTest {

    @ParameterizedTest
    @MethodSource("providerForCombineTest")
    void combineTest(TestSpec input) {
        assertThat(input.predicateSupplier.get().getPredicate()).isEqualTo(input.expected);
    }

    static Collection<TestSpec> providerForCombineTest() {
        return ImmutableList.of(
                TestSpec.of(() -> p2(p("`type`"), p(PLACEHOLDER), EQUALS), "(`type` = ?)"),
                TestSpec.of(() -> p2(p(PLACEHOLDER), p("`type`"), EQUALS), "(? = `type`)"),
                TestSpec.of(
                        () -> p2(p("`type2`"), p(PLACEHOLDER), GREATER_THAN_OR_EQUAL),
                        "(`type2` >= ?)"),
                TestSpec.of(
                        () ->
                                p2(
                                        p2(p("`type1`"), p(PLACEHOLDER), EQUALS),
                                        p2(p("`type2`"), p(PLACEHOLDER), EQUALS),
                                        AND),
                        "((`type1` = ?) AND (`type2` = ?))"),
                TestSpec.of(
                        () ->
                                p2(
                                        p2(p("`type1`"), p(PLACEHOLDER), EQUALS),
                                        p2(p("`type2`"), p(PLACEHOLDER), EQUALS),
                                        OR),
                        "((`type1` = ?) OR (`type2` = ?))"),
                TestSpec.of(
                        () ->
                                p2(
                                        p2(p(PLACEHOLDER), p("`type1`"), EQUALS),
                                        p2(p(PLACEHOLDER), p("`type2`"), EQUALS),
                                        OR),
                        "((? = `type1`) OR (? = `type2`))"),
                TestSpec.of(
                        () ->
                                p2(
                                        p2(p(PLACEHOLDER), p("`type1`"), EQUALS),
                                        p2(p(PLACEHOLDER), p("`type2`"), EQUALS),
                                        AND),
                        "((? = `type1`) AND (? = `type2`))"),
                TestSpec.of(
                        () ->
                                p2(
                                        p2(
                                                p2(
                                                        p(PLACEHOLDER),
                                                        p("`type1`"),
                                                        GREATER_THAN_OR_EQUAL),
                                                p2(p(PLACEHOLDER), p("`type2`"), EQUALS),
                                                AND),
                                        p2(p("`type1`"), p(PLACEHOLDER), GREATER_THAN),
                                        AND),
                        "(((? >= `type1`) AND (? = `type2`)) AND (`type1` > ?))"),
                TestSpec.of(
                        () ->
                                p2(
                                        p2(
                                                p2(
                                                        p2(
                                                                p(PLACEHOLDER),
                                                                p("`type1`"),
                                                                GREATER_THAN_OR_EQUAL),
                                                        p2(p(PLACEHOLDER), p("`type2`"), EQUALS),
                                                        AND),
                                                p2(p("`type1`"), p(PLACEHOLDER), GREATER_THAN),
                                                AND),
                                        p1(p(PLACEHOLDER), IS_NOT_NULL),
                                        OR),
                        "((((? >= `type1`) AND (? = `type2`)) AND (`type1` > ?)) OR (? IS NOT NULL))"));
    }

    private static class TestSpec {
        private final Supplier<ParameterizedPredicate> predicateSupplier;
        private final String expected;

        private TestSpec(Supplier<ParameterizedPredicate> predicateSupplier, String expected) {
            this.predicateSupplier = predicateSupplier;
            this.expected = expected;
        }

        public static TestSpec of(
                Supplier<ParameterizedPredicate> predicateSupplier, String expected) {
            return new TestSpec(predicateSupplier, expected);
        }

        @Override
        public String toString() {
            return expected;
        }
    }

    private static ParameterizedPredicate p(String predicate) {
        return new ParameterizedPredicate(predicate);
    }

    private static ParameterizedPredicate p2(
            ParameterizedPredicate a, ParameterizedPredicate b, FilterPushdownFunction operator) {
        assertThat(operator.isUnaryOperation()).isFalse();
        return a.combine(operator.getFunctionSql(), b);
    }

    private static ParameterizedPredicate p1(
            ParameterizedPredicate predicate, FilterPushdownFunction operator) {
        assertThat(operator.isUnaryOperation()).isTrue();
        if (operator.isOperatorOnTheLeft()) {
            return Optional.of(predicate)
                    .map(
                            fieldPred ->
                                    new ParameterizedPredicate(
                                            String.format(
                                                    "(%s %s)",
                                                    fieldPred.getPredicate(),
                                                    operator.getFunctionSql())))
                    .get();
        } else {
            return Optional.of(predicate)
                    .map(
                            fieldPred ->
                                    new ParameterizedPredicate(
                                            String.format(
                                                    "(%s %s)",
                                                    operator.getFunctionSql(),
                                                    fieldPred.getPredicate())))
                    .get();
        }
    }
}
