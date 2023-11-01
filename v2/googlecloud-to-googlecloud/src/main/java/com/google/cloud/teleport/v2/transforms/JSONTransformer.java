/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;

public abstract class JSONTransformer<T>
    extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public abstract TupleTag<FailsafeElement<T, String>> successTag();

  public abstract TupleTag<FailsafeElement<T, String>> failureTag();

  public static <T> Builder<T> newBuilder() {
    return new AutoValueJSONTransformer.Builder<>();
  }

  public abstract static class Builder<T> {
    public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, String>> successTag);

    public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

    public abstract JSONTransformer<T> build();
  }
  ;

  @Override
  public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
    return elements.apply(
        "ProcessEvents",
        ParDo.of(
                new DoFn<FailsafeElement<T, String>, FailsafeElement<T, String>>() {

                  private List<String> keysToSkip;

                  @Setup
                  public void setup() {
                    keysToSkip = List.of("name", "age");
                  }

                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    FailsafeElement<T, String> element = context.element();
                    String payloadStr = element.getPayload();
                    try {
                      String transformedJson =
                          JSONTransformer.transformJson(payloadStr, keysToSkip);
                      if (!Strings.isNullOrEmpty(transformedJson)) {
                        context.output(
                            FailsafeElement.of(element.getOriginalPayload(), payloadStr));
                      }
                    } catch (Throwable e) {
                      context.output(
                          failureTag(),
                          FailsafeElement.of(element)
                              .setErrorMessage(e.getMessage())
                              .setStacktrace(Throwables.getStackTraceAsString(e)));
                    }
                  }
                })
            .withOutputTags(successTag(), TupleTagList.of(failureTag())));
  }

  public static JsonNode transformJson(JsonNode jsonNode, List<String> keysToSkip) {
    ObjectNode transformedJson = objectMapper.createObjectNode();

    jsonNode
        .fieldNames()
        .forEachRemaining(
            key -> {
              JsonNode value = jsonNode.get(key);

              if (value.isObject()) {
                value = transformJson(value, keysToSkip);
              } else if (!keysToSkip.contains(key)) {
                value = objectMapper.valueToTree(value.asText());
              }

              if (key.contains("timestamp")) {
                long timestampValue = value.asLong();
                String length = String.valueOf(String.valueOf(timestampValue).length());
                switch (length) {
                  case "13":
                    value =
                        objectMapper.valueToTree(Instant.ofEpochMilli(timestampValue).toString());
                    break;
                  case "10":
                    value =
                        objectMapper.valueToTree(Instant.ofEpochSecond(timestampValue).toString());
                    break;
                  case "16":
                    value =
                        objectMapper.valueToTree(
                            Instant.ofEpochSecond(
                                    timestampValue / 1_000_000, (timestampValue % 1_000_000) * 1000)
                                .toString());
                    break;
                }
              }

              if ("null".equals(value.asText())) {
                value = objectMapper.nullNode();
              }

              transformedJson.set(key, value);
            });

    return transformedJson;
  }

  public static String transformJson(String jsonString, List<String> keysToSkip)
      throws IOException {
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    JsonNode transformedJson = transformJson(jsonNode, keysToSkip);
    return objectMapper.writeValueAsString(transformedJson);
  }
}
