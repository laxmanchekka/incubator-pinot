/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Charsets;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


public class DistinctCountAggregationFunction extends BaseSingleInputAggregationFunction<AbstractCollection, Integer> {

  public DistinctCountAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNT;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      return;
    }

    // For non-dictionary-encoded expression
    DataType valueType = blockValSet.getValueType();

    AbstractCollection valueSet = getValueSet(aggregationResultHolder, valueType);
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(stringValues[i].getBytes(Charsets.UTF_8));
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(bytesValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i], valueType).add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i], valueType).add(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i], valueType).add(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i], valueType).add(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i], valueType).add(stringValues[i].getBytes(Charsets.UTF_8));
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i], valueType).add(bytesValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, valueType, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, valueType, groupKeysArray[i], (longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, valueType, groupKeysArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, valueType, groupKeysArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, valueType, groupKeysArray[i], stringValues[i].getBytes(Charsets.UTF_8));
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, valueType, groupKeysArray[i], bytesValues[i]);
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public AbstractCollection extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return emptyCollection();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (AbstractCollection) result;
    }
  }

  private AbstractCollection emptyCollection() {
    return new AbstractCollection() {
      @Override
      public Iterator iterator() {
        return new Iterator() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public Object next() {
            return null;
          }
        };
      }

      @Override
      public int size() {
        return 0;
      }
    };
  }

  @Override
  public AbstractCollection extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return emptyCollection();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (AbstractCollection) result;
    }
  }

  @Override
  public AbstractCollection merge(AbstractCollection intermediateResult1, AbstractCollection intermediateResult2) {
    if (intermediateResult1.getClass().isAssignableFrom(intermediateResult2.getClass())) {
      intermediateResult1.addAll(intermediateResult2);
      return intermediateResult1;
    } else {
      //handle backwards compatibility, we used to use IntHashSet for all datatypes earlier
      //so we try to convert other types into int using hashcode
      //Note this code path is executed only while brokers and servers are getting upgraded.
      //When both are on the same version, they will satisfy the intermediateResult1.getClass().isAssignableFrom(intermediateResult2.getClass() condition
      IntOpenHashSet intOpenHashSet;
      AbstractCollection toMerge;
      if (intermediateResult1 instanceof IntOpenHashSet) {
        intOpenHashSet = (IntOpenHashSet) intermediateResult1;
        toMerge = intermediateResult2;
      } else {
        intOpenHashSet = (IntOpenHashSet) intermediateResult2;
        toMerge = intermediateResult1;
      }
      if (toMerge instanceof LongOpenHashSet) {
        LongOpenHashSet longOpenHashSet = (LongOpenHashSet) toMerge;
        for (long e : longOpenHashSet) {
          intOpenHashSet.add(Long.hashCode(e));
        }
      } else if (toMerge instanceof FloatOpenHashSet) {
        FloatOpenHashSet floatOpenHashSet = (FloatOpenHashSet) toMerge;
        for (float e : floatOpenHashSet) {
          intOpenHashSet.add(Float.hashCode(e));
        }
      } else if (toMerge instanceof DoubleOpenHashSet) {
        DoubleOpenHashSet doubleOpenHashSet = (DoubleOpenHashSet) toMerge;
        for (double e : doubleOpenHashSet) {
          intOpenHashSet.add(Double.hashCode(e));
        }
      } else if (toMerge instanceof ObjectOpenHashSet) {
        ObjectOpenHashSet objectOpenHashSet = (ObjectOpenHashSet) toMerge;
        for (Object e : objectOpenHashSet) {
          intOpenHashSet.add(e.hashCode());
        }
      }
      return intOpenHashSet;
    }
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(AbstractCollection intermediateResult) {
    return intermediateResult.size();
  }

  /**
   * Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value set from the result holder or creates a new one if it does not exist.
   */
  protected static AbstractCollection getValueSet(AggregationResultHolder aggregationResultHolder, DataType valueType) {
    AbstractCollection valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = getAbstractCollection(valueType);
      aggregationResultHolder.setValue(valueSet);
    }
    return valueSet;
  }

  private static AbstractCollection getAbstractCollection(DataType valueType) {
    AbstractCollection valueSet;
    switch (valueType) {
      case INT:
        valueSet = new IntOpenHashSet();
        break;
      case LONG:
        valueSet = new LongOpenHashSet();
        break;
      case FLOAT:
        valueSet = new FloatOpenHashSet();
        break;
      case DOUBLE:
        valueSet = new DoubleOpenHashSet();
        break;
      case STRING:
        valueSet = new ObjectOpenHashSet<byte[]>();
        break;
      case BYTES:
        valueSet = new ObjectOpenHashSet<byte[]>();
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
    return valueSet;
  }

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value set for the given group key or creates a new one if it does not exist.
   */
  protected static AbstractCollection getValueSet(GroupByResultHolder groupByResultHolder, int groupKey,
      DataType valueType) {
    AbstractCollection valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = getAbstractCollection(valueType);
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    return valueSet;
  }

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /**
   * Helper method to set value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, DataType valueType, int[] groupKeys,
      int value) {
    for (int groupKey : groupKeys) {
      getValueSet(groupByResultHolder, groupKey, valueType).add(value);
    }
  }

  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, DataType valueType, int[] groupKeys,
      long value) {
    for (int groupKey : groupKeys) {
      getValueSet(groupByResultHolder, groupKey, valueType).add(value);
    }
  }

  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, DataType valueType, int[] groupKeys,
      float value) {
    for (int groupKey : groupKeys) {
      getValueSet(groupByResultHolder, groupKey, valueType).add(value);
    }
  }

  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, DataType valueType, int[] groupKeys,
      double value) {
    for (int groupKey : groupKeys) {
      getValueSet(groupByResultHolder, groupKey, valueType).add(value);
    }
  }

  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, DataType valueType, int[] groupKeys,
      byte[] value) {
    for (int groupKey : groupKeys) {
      getValueSet(groupByResultHolder, groupKey, valueType).add(value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
   * expression.
   */
  private static AbstractCollection convertToValueSet(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType valueType = dictionary.getValueType();
    switch (valueType) {
      case INT:
        IntOpenHashSet intOpenHashSet = new IntOpenHashSet(dictIdBitmap.getCardinality());
        while (iterator.hasNext()) {
          intOpenHashSet.add(dictionary.getIntValue(iterator.next()));
        }
        return intOpenHashSet;
      case LONG:
        LongOpenHashSet longOpenHashSet = new LongOpenHashSet(dictIdBitmap.getCardinality());
        while (iterator.hasNext()) {
          longOpenHashSet.add(dictionary.getLongValue(iterator.next()));
        }
        return longOpenHashSet;
      case FLOAT:
        FloatOpenHashSet floatOpenHashSet = new FloatOpenHashSet(dictIdBitmap.getCardinality());
        while (iterator.hasNext()) {
          floatOpenHashSet.add(dictionary.getFloatValue(iterator.next()));
        }
        return floatOpenHashSet;

      case DOUBLE:
        DoubleOpenHashSet doubleOpenHashSet = new DoubleOpenHashSet(dictIdBitmap.getCardinality());
        while (iterator.hasNext()) {
          doubleOpenHashSet.add(dictionary.getDoubleValue(iterator.next()));
        }
        return doubleOpenHashSet;
      case STRING:
        ObjectOpenHashSet<byte[]> stringObjectOpenHashSet =
            new ObjectOpenHashSet<byte[]>(dictIdBitmap.getCardinality());
        while (iterator.hasNext()) {
          stringObjectOpenHashSet.add(dictionary.getStringValue(iterator.next()).getBytes(Charsets.UTF_8));
        }
        return stringObjectOpenHashSet;

      case BYTES:
        ObjectOpenHashSet<byte[]> bytesObjectOpenHashSet =
            new ObjectOpenHashSet<byte[]>(dictIdBitmap.getCardinality());

        while (iterator.hasNext()) {
          bytesObjectOpenHashSet.add((dictionary.getBytesValue(iterator.next())));
        }
        return bytesObjectOpenHashSet;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new RoaringBitmap();
    }
  }
}
