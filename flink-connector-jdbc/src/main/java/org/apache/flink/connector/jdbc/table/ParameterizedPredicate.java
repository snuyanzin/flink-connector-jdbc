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

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;

import static org.apache.flink.connector.jdbc.table.JdbcFilterPushdownPreparedStatementVisitor.PLACEHOLDER;

/** A data class that model parameterized sql predicate. */
@Experimental
public class ParameterizedPredicate {
    private String predicate;
    private Serializable[] parameters;
    private int[] indexesOfPredicatePlaceHolders;
    private int indexArrayLength;

    public int[] getIndexesOfPredicatePlaceHolders() {
        return indexesOfPredicatePlaceHolders;
    }

    public void setIndexesOfPredicatePlaceHolders(int[] indexesOfPredicatePlaceHolders) {
        this.indexesOfPredicatePlaceHolders = indexesOfPredicatePlaceHolders;
    }

    public ParameterizedPredicate(String predicate) {
        this.predicate = predicate;
        this.parameters = new Serializable[0];
    }

    public ParameterizedPredicate(ParameterizedPredicate that) {
        this.predicate = that.getPredicate();
        this.parameters = that.getParameters();
        this.indexesOfPredicatePlaceHolders = that.getIndexesOfPredicatePlaceHolders();
    }

    public Serializable[] getParameters() {
        return parameters;
    }

    public void setParameters(Serializable[] parameters) {
        this.parameters = parameters;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public ParameterizedPredicate combine(String operator, ParameterizedPredicate that) {
        int indexLength =
                (indexesOfPredicatePlaceHolders == null
                        ? 0
                        : indexesOfPredicatePlaceHolders.length);
        for (int i = 0; i < indexLength; i++) {
            this.indexesOfPredicatePlaceHolders[i] = this.indexesOfPredicatePlaceHolders[i] + 1;
        }
        StringBuilder strPredicate =
                new StringBuilder("(")
                        .append(this.predicate)
                        .append(" ")
                        .append(operator)
                        .append(" ");
        final int sbLength = strPredicate.length();
        if (PLACEHOLDER.equals(this.predicate)) {
            this.indexesOfPredicatePlaceHolders =
                    ArrayUtils.add(this.indexesOfPredicatePlaceHolders, 1);
        } else if (PLACEHOLDER.equals(that.predicate)) {
            this.indexesOfPredicatePlaceHolders =
                    ArrayUtils.add(this.indexesOfPredicatePlaceHolders, sbLength);
        }

        if (that.indexesOfPredicatePlaceHolders != null
                && that.indexesOfPredicatePlaceHolders.length > 0) {
            int[] newArrayOfIndexes;
            int offset = 0;
            if (this.indexesOfPredicatePlaceHolders == null
                    || this.indexesOfPredicatePlaceHolders.length == 0) {
                newArrayOfIndexes = that.indexesOfPredicatePlaceHolders;
            } else {
                newArrayOfIndexes =
                        new int
                                [this.indexesOfPredicatePlaceHolders.length
                                        + that.indexesOfPredicatePlaceHolders.length];
                System.arraycopy(
                        this.indexesOfPredicatePlaceHolders,
                        0,
                        newArrayOfIndexes,
                        0,
                        this.indexesOfPredicatePlaceHolders.length);
                offset = this.indexesOfPredicatePlaceHolders.length;
            }
            for (int i = 0; i < that.indexesOfPredicatePlaceHolders.length; i++) {
                newArrayOfIndexes[i + offset] = that.indexesOfPredicatePlaceHolders[i] + sbLength;
            }
            this.indexesOfPredicatePlaceHolders = newArrayOfIndexes;
        }

        this.predicate = strPredicate.append(that.predicate).append(")").toString();
        this.parameters = ArrayUtils.addAll(this.parameters, that.parameters);

        return this;
    }
}
