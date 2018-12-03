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
package org.apache.beam.runners.flink.translation;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.flink.translation.utils.FlinkPipelineTranslatorUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Tests for {@link org.apache.beam.runners.flink.translation.utils.FlinkPipelineTranslatorUtils}.
 */
public class FlinkPipelineTranslatorUtilsTest implements Serializable {

  @Test
  /* Test for generating readable operator names during translation. */
  public void testOperatorNameGeneration() throws Exception {
    Pipeline p = Pipeline.create();
    p.apply(Impulse.create())
        // Anonymous ParDo
        .apply(
            ParDo.of(
                new DoFn<byte[], String>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<String> outputReceiver) {}
                }))
        // Name ParDo
        .apply(
            "MyName",
            ParDo.of(
                new DoFn<String, Integer>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<Integer> outputReceiver) {}
                }))
        .apply(
            // This is how Python pipelines construct names
            "ref_AppliedPTransform_count",
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<Integer> outputReceiver) {}
                }));

    ExecutableStage firstEnvStage =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p))
            .getFusedStages()
            .stream()
            .findFirst()
            .get();
    RunnerApi.ExecutableStagePayload basePayload =
        RunnerApi.ExecutableStagePayload.parseFrom(
            firstEnvStage.toPTransform("foo").getSpec().getPayload());

    String executableStageName =
        FlinkPipelineTranslatorUtils.genOperatorNameFromStagePayload(basePayload);

    assertThat(executableStageName, is("[3]{ParDo(Anonymous), MyName, count}"));
  }

  @Test
  /* Test for generating readable operator names during translation. */
  public void testNestedOperatorNameGeneration() throws Exception {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs("--experiments=bean_fnapi", "--runner=FlinkRunner").create());
    p.getOptions().as(ExperimentalOptions.class).setExperiments(Lists.newArrayList("bean_fnapi"));
    p
        /*
        .apply(TextIO.read().from("path/to/nowhere"))
        */
        .apply(Impulse.create())
        .apply(MapElements.into(TypeDescriptor.of(String.class)).via(impulse -> "line"))
        .apply(Count.perElement())
        .apply("Format", MapElements.via(new SimpleFunction<KV<String, Long>, String>(counts -> counts.getKey() + ": " + counts.getValue()) {}))
        .apply(new org.apache.beam.sdk.transforms.PTransform<PCollection<String>, POutput>() {
          @Override
          public POutput expand(PCollection<String> input) {
            input.apply("A", Count.perElement()).apply(Count.perElement());
            return input.apply("B", Count.perElement());
          }
        });
        ; //.apply(TextIO.write().to("path/to/elsewhere"));
        /*
        // Anonymous ParDo
        .apply(
            ParDo.of(
                new DoFn<byte[], String>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<String> outputReceiver) {}
                }))
        // Name ParDo
        .apply(
            "MyName",
            ParDo.of(
                new DoFn<String, Integer>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<Integer> outputReceiver) {}
                }))
        .apply(
            // This is how Python pipelines construct names
            "ref_AppliedPTransform_count",
            ParDo.of(
                new DoFn<Integer, Integer>() {
                  @ProcessElement
                  public void processElement(
                      ProcessContext processContext, OutputReceiver<Integer> outputReceiver) {}
                }));*/

    Set<String> executableStageNames =
        GreedyPipelineFuser.fuse(PipelineTranslation.toProto(p))
            .getFusedStages()
            .stream()
            .map(stage -> stage.toPTransform("foo"))
            .map(FlinkPipelineTranslatorUtilsTest::parsePayload)
            .map(FlinkPipelineTranslatorUtils::genOperatorNameFromStagePayload)
        .collect(Collectors.toSet());

    System.out.println(Joiner.on("\n").join(executableStageNames));

    assertThat(executableStageNames, containsInAnyOrder(
        "[2]{MapElements, Count.PerElement}",
        "[4]{Count.PerElement, Format, AnonymousTransform/{A, B}}",
        "[1]{AnonymousTransform}",
        "[1]{AnonymousTransform}"));
  }

  private static ExecutableStagePayload parsePayload(PTransform proto) {
    try {
      return RunnerApi.ExecutableStagePayload.parseFrom(proto.getSpec().getPayload());
    } catch (Exception exn) {
      throw new RuntimeException(exn);
    }
  }
}
