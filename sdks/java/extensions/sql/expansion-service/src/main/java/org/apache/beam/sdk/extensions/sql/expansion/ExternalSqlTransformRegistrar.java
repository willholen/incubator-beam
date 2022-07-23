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
package org.apache.beam.sdk.extensions.sql.expansion;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoService(ExternalTransformRegistrar.class)
public class ExternalSqlTransformRegistrar implements ExternalTransformRegistrar {
  private static final String URN = "beam:external:java:sql:v1";
  private static final ImmutableMap<String, Class<? extends QueryPlanner>> DIALECTS =
      ImmutableMap.<String, Class<? extends QueryPlanner>>builder()
          .put("zetasql", ZetaSQLQueryPlanner.class)
          .put("calcite", CalciteQueryPlanner.class)
          .build();

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap.of(
        URN, Builder.class);
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class Configuration {
    String query = "";
    @Nullable String dialect;

    public void setQuery(String query) {
      this.query = query;
    }

    public void setDialect(@Nullable String dialect) {
      this.dialect = dialect;
    }
  }

  private static class Builder
      implements ExternalTransformBuilder<Configuration, PInput, PCollection<Row>> {
    @Override
    public PTransform<PInput, PCollection<Row>> buildExternal(Configuration configuration) {
      SqlTransform transform = SqlTransform.query(configuration.query);
      if (configuration.dialect != null) {
        Class<? extends QueryPlanner> queryPlanner =
            DIALECTS.get(configuration.dialect.toLowerCase());
        if (queryPlanner == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Received unknown SQL Dialect '%s'. Known dialects: %s",
                  configuration.dialect, DIALECTS.keySet()));
        }
        transform = transform.withQueryPlannerClass(queryPlanner);
      }
      return transform;
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class SqlSchemaTransform extends TypedSchemaTransformProvider<Configuration> {
    @Override
    public String identifier() {
      return "sql";
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of("input");
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of("output");
    }

    @Override
    protected Class<Configuration> configurationClass() {
      return Configuration.class;
    }

    @Override
    protected SchemaTransform from(Configuration configuration) {
      return new SchemaTransform() {
        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              configuration.setQuery("SELECT " + configuration.query + " FROM PCOLLECTION");
              return PCollectionRowTuple.of(
                  "output", input.get("input").apply(new Builder().buildExternal(configuration)));
            }
          };
        }
      };
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class FilePathConfig {
    String path = "";

    public void setPath(String path) {
      this.path = path;
    }
  }

  // A portable runner is required to use these cross-language
  // transforms. Python ULR with the direct_embed_docker_python
  // option set allows local files to be accessed.
  // (LOOPBACK mode is useful as well.)
  // This only impacts the run command--construction and validation
  // still work fine.
  public abstract static class ReadPansasSchemaTransform
      extends TypedSchemaTransformProvider<FilePathConfig> {
    public abstract String format();

    public Map<String, Object> extraKwargs() {
      return ImmutableMap.of();
    }

    @Override
    public String identifier() {
      return "read_" + format();
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of();
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of("output");
    }

    @Override
    protected Class<FilePathConfig> configurationClass() {
      return FilePathConfig.class;
    }

    @Override
    protected SchemaTransform from(FilePathConfig config) {
      return new SchemaTransform() {
        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              return PCollectionRowTuple.of(
                  "output",
                  (PCollection<Row>)
                      input
                          .getPipeline()
                          .apply(
                              PythonExternalTransform.from("apache_beam.dataframe.io.ReadViaPandas")
                                  .withKwarg("format", format())
                                  .withKwarg("path", config.path)
                                  .withKwargs(extraKwargs())));
            }
          };
        }
      };
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class ReadCsv extends ReadPansasSchemaTransform {
    @Override
    public String format() {
      return "csv";
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class ReadJson extends ReadPansasSchemaTransform {
    @Override
    public String format() {
      return "json";
    }

    @Override
    public Map<String, Object> extraKwargs() {
      return ImmutableMap.of("orient", "records", "lines", true);
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class ReadParquet extends ReadPansasSchemaTransform {
    @Override
    public String format() {
      return "parquet";
    }
  }

  public abstract static class WritePansasSchemaTransform
      extends TypedSchemaTransformProvider<FilePathConfig> {
    public abstract String format();

    public Map<String, Object> extraKwargs() {
      return ImmutableMap.of();
    }

    @Override
    public String identifier() {
      return "write_" + format();
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of("input");
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of();
    }

    @Override
    protected Class<FilePathConfig> configurationClass() {
      return FilePathConfig.class;
    }

    @Override
    protected SchemaTransform from(FilePathConfig config) {
      return new SchemaTransform() {
        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              input
                      .get("input")
                      .apply(
                              PythonExternalTransform.from(
                                      "apache_beam.dataframe.io.WriteViaPandas")
                                      .withKwarg("format", format())
                                      .withKwarg("path", config.path)
                                      .withKwargs(extraKwargs()));
              return PCollectionRowTuple.empty(input.getPipeline());
            }
          };
        }
      };
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class WriteCsv extends WritePansasSchemaTransform {
    @Override
    public String format() {
      return "csv";
    }

    @Override
    public Map<String, Object> extraKwargs() {
      return ImmutableMap.of("index", false);
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class WriteJson extends WritePansasSchemaTransform {
    @Override
    public String format() {
      return "json";
    }

    @Override
    public Map<String, Object> extraKwargs() {
      return ImmutableMap.of("orient", "records", "lines", true);
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class WriteParquet extends WritePansasSchemaTransform {
    @Override
    public String format() {
      return "parquet";
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class ReadFromTextSchemaTransform
      extends TypedSchemaTransformProvider<FilePathConfig> {

    @Override
    public String identifier() {
      return "read_text";
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of();
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of("output");
    }

    @Override
    protected Class<FilePathConfig> configurationClass() {
      return FilePathConfig.class;
    }

    private static final Schema LINE_SCHEMA =
        Schema.of(Schema.Field.of("line", Schema.FieldType.STRING));

    private static class MapToLinesDoFn extends DoFn<String, Row> {
      @ProcessElement
      public void processElement(@Element String line, OutputReceiver<Row> out) {
        out.output(Row.withSchema(LINE_SCHEMA).addValue(line).build());
      }
    }

    @Override
    protected SchemaTransform from(FilePathConfig config) {
      return new SchemaTransform() {
        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              return PCollectionRowTuple.of(
                  "output",
                  (PCollection<Row>)
                      input
                          .getPipeline()
                          .apply(TextIO.read().from(config.path))
                          .apply(ParDo.of(new MapToLinesDoFn()))
                          .setRowSchema(LINE_SCHEMA));
            }
          };
        }
      };
    }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class WriteToTextSchemaTransform
          extends TypedSchemaTransformProvider<FilePathConfig> {

    @Override
    public String identifier() {
      return "write_text";
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of("input");
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of();
    }

    @Override
    protected Class<FilePathConfig> configurationClass() {
      return FilePathConfig.class;
    }

    @Override
    protected SchemaTransform from(FilePathConfig config) {
      return new SchemaTransform() {
        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              input.get("input").apply(ToString.elements()).apply(TextIO.write().to(config.path));
              return PCollectionRowTuple.empty(input.getPipeline());
            }
          };
        }
      };
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class IdentityConfig {
    public String somethingToAvoidError;
    public void setSomethingToAvoidError(String s) {somethingToAvoidError = s; }
  }

  @AutoService(SchemaTransformProvider.class)
  public static class IdentityTransform
          extends TypedSchemaTransformProvider<IdentityConfig> {

    @Override
    public String identifier() {
      return "identity";
    }

    @Override
    public List<String> inputCollectionNames() {
      return ImmutableList.of("input");
    }

    @Override
    public List<String> outputCollectionNames() {
      return ImmutableList.of("output");
    }

    @Override
    protected Class<IdentityConfig> configurationClass() {
      return IdentityConfig.class;
    }

    @Override
    protected SchemaTransform from(IdentityConfig config) {
      return new SchemaTransform() {
        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              // Using Flatten because Java might have issues with transforms returning inputs.
              return PCollectionRowTuple.of(
                      "output",
                              PCollectionList.of(input.get("input")).apply(
                              Flatten.pCollections()));
            }
          };
        }
      };
    }
  }
}
