# Cadence dbt Project
This project represents a dbt project where raw source data is transformed into reliable data models suitable for ongoing analytics.

## Design & Strategy

At a high level, it follows a 3-layered Medallion Architecture:
- **Bronze**: Unformatted data ingested directly from source systems _(Staging)_
- **Silver**: Transformed Data Models _(Refined)_
- **Gold**: Custom tables intended for Analytics use cases _(Marts)_

## Getting Started
The below instructions are for getting setup with `dbt core` on your local machine.

### Environment Setup
    
1. Confirm connection
    1. Navigate to dbt project `cd cadence`
    2. Run `dbt debug`

3. Run `dbt deps` to get packages

4. You’re all set!

## Support
For any questions or support related to this project, please reach out Marc Arguijo
- marcos.a.arguijo@gmail.com

# DBT Styles

## 1. Column Naming
- Use **snake_case** for all column names.
- When exporting to Power BI, convert column names to **Title Case**.

## 2. CTE (Common Table Expressions)
- Assign **meaningful names** to all CTEs to clearly indicate their purpose.
- If you need to use `SELECT *`, keep it in the main query. For specific columns, create a CTE.

## 3. Comments and Headers
- Always include a **header** at the top of each model file, describing the model's purpose and any relevant information.
- Add **comments** throughout your SQL to explain complex logic or important decisions.

## 4. Table and Relationship Naming
- Use **plural** names for all tables.
- For relationship tables (JOINS), use the table name directly to clarify the relationship.
- If you do not use the table name, ensure the relationship is documented in the header.


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
