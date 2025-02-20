/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.database;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.enums.EdgeNodesMatchMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.enums.SaveMode;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.List;
import java.util.Set;
import org.junit.Test;

// TODO: manually instantiate Target instances instead of deserializing JSON spec
public class CypherGeneratorTest {

  private static final String SPEC_PATH = "src/test/resources/testing-specs/cypher-generator-test";

  @Test
  public void testFixIdentifierFirstCharAndSpaces() {
    String testExpression = "1Customer Id";
    String testExpressionValidated = ModelUtils.makeValidNeo4jIdentifier(testExpression);
    assertThat(testExpressionValidated).isEqualTo("_1Customer_Id");
  }

  @Test
  public void specifiesKeysInRelationshipMergePattern() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/single-target-relation-import-with-keys-spec.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();
    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MATCH (source:Source {id: row.source}) "
                + "MATCH (target:Target {id: row.target}) "
                + "MERGE (source)-[rel:LINKS {rel_id1: row.rel_id_1,rel_id2: row.rel_id_2}]->(target) "
                + "SET rel += {ts: row.timestamp}");
  }

  @Test
  public void specifiesOnlyTypeInKeylessRelationshipMergePattern() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/single-target-relation-import-without-keys-spec.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();
    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MATCH (source:Source {id: row.source}) "
                + "MATCH (target:Target {id: row.target}) "
                + "MERGE (source)-[rel:LINKS]->(target) "
                + "SET rel += {ts: row.timestamp}");
  }

  @Test
  public void mergesEdgesAsWellAsTheirStartAndEndNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/single-target-relation-import-merge-all.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MERGE (source:Source {src_id: row.source}) "
                + "MERGE (target:Target {tgt_id: row.target}) "
                + "MERGE (source)-[rel:LINKS]->(target) SET rel += {ts: row.timestamp}");
  }

  @Test
  public void createsEdgesAndMergesTheirStartAndEndNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(
            SPEC_PATH + "/single-target-relation-import-create-rels-merge-nodes.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    String statement = CypherGenerator.getUnwindCreateCypher(relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row  "
                + "MERGE (source:Source {src_id: row.source}) "
                + "MERGE (target:Target {tgt_id: row.target}) "
                + "CREATE (source)-[rel:LINKS]->(target) SET rel += {ts: row.timestamp}");
  }

  @Test
  public void generatesNodeKeyConstraintsWhenMergingEdgeAndItsNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/single-target-relation-import-merge-all.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    Set<String> statements =
        CypherGenerator.getIndexAndConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id IS NODE KEY"));
  }

  @Test
  public void generatesNodeKeyConstraintsWhenCreatingEdgeAndMergingItsNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(
            SPEC_PATH + "/single-target-relation-import-create-rels-merge-nodes.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    Set<String> statements =
        CypherGenerator.getIndexAndConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id IS NODE KEY"));
  }

  @Test
  public void generatesMultiLabelNodeKeyConstraintsWhenMergingEdgeNodes() {
    JobSpec jobSpec = JobSpecMapper.fromUri(SPEC_PATH + "/multi-label-single-pass-import.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    Set<String> statements =
        CypherGenerator.getIndexAndConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source1) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source2) REQUIRE n.src_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target1) REQUIRE n.tgt_id IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target2) REQUIRE n.tgt_id IS NODE KEY"));
  }

  @Test
  public void generatesMultiDistinctKeysNodeKeyConstraintsWhenMergingEdgeNodes() {
    JobSpec jobSpec =
        JobSpecMapper.fromUri(SPEC_PATH + "/multi-distinct-keys-single-pass-import.json");
    Target relationshipTarget = jobSpec.getTargets().iterator().next();

    Set<String> statements =
        CypherGenerator.getIndexAndConstraintsCypherStatements(relationshipTarget);

    assertThat(statements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id1 IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Source) REQUIRE n.src_id2 IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id1 IS NODE KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Target) REQUIRE n.tgt_id2 IS NODE KEY"));
  }

  @Test
  public void doesNotGenerateDuplicateStatementForSelfLinkingNodes() {
    Target target = new Target();
    target.setSaveMode(SaveMode.merge);
    target.setEdgeNodesMatchMode(EdgeNodesMatchMode.merge);
    target.setType(TargetType.edge);
    target.setName("self-linking-nodes");
    Mapping type = new Mapping();
    type.setFragmentType(FragmentType.rel);
    type.setRole(RoleType.type);
    type.setConstant("SELF_LINKS_TO");
    Mapping key = new Mapping();
    key.setFragmentType(FragmentType.rel);
    key.setRole(RoleType.key);
    key.setName("targetRelProperty");
    key.setField("source_field");
    Mapping sourceLabel = new Mapping();
    sourceLabel.setFragmentType(FragmentType.source);
    sourceLabel.setRole(RoleType.label);
    sourceLabel.setConstant("PlaceholderLabel");
    Mapping sourceKey = new Mapping();
    sourceKey.setFragmentType(FragmentType.source);
    sourceKey.setRole(RoleType.key);
    sourceKey.setName("targetNodeProperty");
    sourceKey.setField("source_node_field");
    Mapping targetLabel = new Mapping();
    targetLabel.setFragmentType(FragmentType.target);
    targetLabel.setRole(RoleType.label);
    targetLabel.setConstant("PlaceholderLabel");
    Mapping targetKey = new Mapping();
    targetKey.setFragmentType(FragmentType.target);
    targetKey.setRole(RoleType.key);
    targetKey.setName("targetNodeProperty");
    targetKey.setField("source_node_field");
    target.setMappings(List.of(type, key, sourceLabel, sourceKey, targetLabel, targetKey));

    Set<String> schemaStatements = CypherGenerator.getIndexAndConstraintsCypherStatements(target);

    assertThat(schemaStatements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:SELF_LINKS_TO]-() REQUIRE r.targetRelProperty IS RELATIONSHIP KEY",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:PlaceholderLabel) REQUIRE n.targetNodeProperty IS NODE KEY"));
  }
}
