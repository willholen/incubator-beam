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
package org.apache.beam.runners.flink.translation.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.InvalidProtocolBufferException;

/** Utilities for pipeline translation. */
public final class FlinkPipelineTranslatorUtils {
  private FlinkPipelineTranslatorUtils() {}

  /** Creates a mapping from PCollection id to output tag integer. */
  public static BiMap<String, Integer> createOutputMap(Iterable<String> localOutputs) {
    ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
    int outputIndex = 0;
    // sort localOutputs for stable indexing
    for (String tag : Sets.newTreeSet(localOutputs)) {
      builder.put(tag, outputIndex);
      outputIndex++;
    }
    return builder.build();
  }

  /** Creates a coder for a given PCollection id from the Proto definition. */
  public static <T> Coder<WindowedValue<T>> instantiateCoder(
      String collectionId, RunnerApi.Components components) {
    PipelineNode.PCollectionNode collectionNode =
        PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
    try {
      return WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate Coder", e);
    }
  }

  public static WindowingStrategy getWindowingStrategy(
      String pCollectionId, RunnerApi.Components components) {
    RunnerApi.WindowingStrategy windowingStrategyProto =
        components.getWindowingStrategiesOrThrow(
            components.getPcollectionsOrThrow(pCollectionId).getWindowingStrategyId());

    final WindowingStrategy<?, ?> windowingStrategy;
    try {
      return WindowingStrategyTranslation.fromProto(
          windowingStrategyProto, RehydratedComponents.forComponents(components));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate windowing strategy %s for %s.",
              windowingStrategyProto, pCollectionId),
          e);
    }
  }

  public static String genOperatorNameFromStagePayload(
      RunnerApi.ExecutableStagePayload stagePayload) {
    StringBuilder sb = new StringBuilder();
    final int transformsCount = stagePayload.getTransformsCount();
    sb.append("[").append(transformsCount).append("]");
    sb.append("{");
    List<String> stageNames = new ArrayList<>();
    for (int i = 0; i < transformsCount; i++) {
      String id = stagePayload.getTransforms(i);
      String name = stagePayload.getComponents().getTransformsOrThrow(id).getUniqueName();
      // Python: Remove the 'ref_AppliedPTransform_' prefix which just makes the name longer
      name = name.replaceFirst("^ref_AppliedPTransform_", "");
      // Java: Remove the 'ParMultiDo(Anonymous)' suffix which just makes the name longer
      name = name.replaceFirst("/ParMultiDo\\(Anonymous\\)$", "");
      stageNames.add(name);
      //sb.append(name);
      if (i + 1 < transformsCount) {
        //sb.append(", ");
      }
    }
    sb.append(compressedNames(stageNames));
    sb.append("}");
    return sb.toString();
  }

  public static String compressedNames(Collection<String> fullNames) {
    Multimap<String, String> stagesByOuterStage = LinkedHashMultimap.create();
    for(String fullName : fullNames) {
      int slash = fullName.indexOf('/');
      if (slash == -1) {
        stagesByOuterStage.put(fullName, "");
      } else {
        stagesByOuterStage.put(fullName.substring(0, slash), fullName.substring(slash + 1));
      }
    }
      return Joiner.on(", ").join(stagesByOuterStage.asMap().entrySet().stream().map(
          stage -> stage.getValue().size() == 1 ? stage.getKey() + stage.getValue() : stagesByOuterStage.size()  == 1 ? compressedNames(stage.getValue()) : String.format("%s/{%s}", stage.getKey(), compressedNames(stage.getValue()))).collect(Collectors.toList()));
  }
}
