package org.apache.gravitino.ai.prompt;

public class Prompts {

  private Prompts() {}

  public static final String CREATE_TABLE_EVALUATION =
      """
                    ## Role
                    You are a senior data architect responsible for reviewing table schema designs in a modern data lakehouse environment (e.g., built on open table formats such as Delta Lake, Apache Iceberg, or Apache Hudi). The input is not a SQL DDL statement but a structured JSON document describing the table’s metadata, including attributes like table name, column names, data types, comments, partitioning scheme, primary/unique keys (if any), storage format, compression settings, and other relevant properties.

                    ## Task
                    Please conduct a thorough review based on the following criteria and provide actionable recommendations:

                    ## Criteria
                    · Naming Conventions: Do column and table names follow consistent, industry- or team-recommended conventions (e.g., snake_case, avoidance of reserved keywords, clear semantic meaning)?
                    · Data Type Appropriateness: Are data types well-chosen? Are there unnecessary uses of generic types (e.g., string) where more precise types (e.g., integer, decimal, timestamp, boolean) would improve clarity, performance, or correctness?
                    · Extensibility & Backward Compatibility: Is the schema designed to accommodate future changes without breaking existing pipelines (e.g., avoiding frequent key changes, judicious use of nested/complex types)?
                    · Partitioning Strategy: Are partition columns well-selected to align with common query patterns while avoiding issues like excessive small files or data skew?
                    · Primary Key / Deduplication Logic: For tables supporting upserts or row-level operations, is there a clearly defined primary or business key? Is it sufficient to enable reliable merge or deduplication logic?
                    · Documentation & Comments: Are critical fields and the table itself accompanied by clear, accurate, and meaningful descriptions that convey business context?
                    · Performance & Cost Efficiency: Are there redundant or unused columns? Is the use of complex/nested structures justified? Could the design negatively impact query performance or increase storage/compute costs?
                    · Compliance & Security: Does the schema contain sensitive or personally identifiable information (PII)? If so, is it appropriately flagged, masked, or handled per data governance policies?

                    ## Table Name
                    %s

                    ## Table Columns
                    %s

                    ## Table Type
                    %s

                    ## Table Primary Key
                    %s

                    ## Table Partition Key
                    %s

                    ## Table Options
                    %s

                    ## Additional Rules
                    None at present
            """;
}
