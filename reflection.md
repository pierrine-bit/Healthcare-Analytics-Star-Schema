
## 1. OLTP vs OLAP Performance Differences

This project helped me clearly understand the difference between OLTP and OLAP systems, particularly from a performance perspective. By analyzing real queries and execution plans, I observed how join complexity and data structure directly impact query performance.

In the OLTP schema, most analytical queries required multiple joins across tables such as encounters, providers, billing, and specialties. This increased query complexity and slowed down aggregation.

After redesigning the schema into a star schema, I observed clear performance improvements for most queries:

- Revenue query improved from ~24 ms to ~7 ms  
- Encounters by specialty improved from ~20–40 ms to ~5 ms  

These improvements were mainly due to:
- Reduced join complexity  
- Pre-aggregation of metrics (e.g., revenue)  
- Use of a date dimension instead of runtime calculations  

However, not all queries improved. The diagnosis–procedure query became slower (~68 ms vs ~16 ms). This is due to row explosion caused by many-to-many relationships, which cannot be eliminated by schema design alone.

This demonstrates that while star schemas are highly effective for aggregation queries, they do not always improve performance for highly combinatorial queries.

---

## 2. Trade-offs Between Normalization and Denormalization

Working with both schemas helped me understand the trade-offs clearly.

### OLTP (Normalized Design):
- High data integrity  
- Minimal redundancy  
- Optimized for transactional operations  
- Complex and slower analytical queries  

### Star Schema (Denormalized Design):
- Simpler query structure  
- Faster analytical performance  
- Some data redundancy  
- Requires ETL processes for data consistency  

I learned that the star schema is specifically designed for analytical workloads, where read performance is prioritized over write efficiency.

---

## 3. Challenges Faced and Solutions

### a) Row Explosion

While analyzing the diagnosis–procedure query, I observed a significant increase in row counts due to many-to-many relationships.

**Solution:**  
I implemented bridge tables to manage these relationships without duplicating data in the fact table.

---

### b) ETL Errors (Foreign Keys and Duplicates)

During ETL implementation, I encountered foreign key violations and duplicate key errors.

**Solution:**  
- Loaded dimension tables before the fact table  
- Used TRUNCATE for clean reloads  
- Applied `ON CONFLICT DO NOTHING` to prevent duplicates  

---

### c) Date Handling

Initially, queries relied on functions such as `DATE_TRUNC`, which reduced performance.

**Solution:**  
I introduced a date dimension with precomputed attributes (year, month), simplifying queries and improving efficiency.

---

### d) Historical Tracking

The initial design did not support tracking changes in provider attributes.

**Solution:**  
I implemented a Slowly Changing Dimension (SCD Type 2) structure in the provider dimension to support historical analysis.

---

## 4. Potential Improvements

Although the current solution performs well, several improvements can be made:

- Add more composite indexes to further optimize queries  
- Partition the fact table by date for scalability  
- Implement incremental ETL instead of full reload  

Additionally, advanced optimization techniques could be applied, such as:
- Using window functions (e.g., LAG or LEAD) to improve readmission analysis  
- Leveraging materialized views or precomputed aggregates for complex many-to-many queries  

These improvements would further enhance performance and scalability in a production environment.

---

## Conclusion

This project provided practical experience in transforming a transactional database into an analytical model. It demonstrated how schema design, ETL processes, and indexing strategies work together to improve performance.

It also showed that there is no single optimal design. Each approach involves trade-offs depending on the type of workload.

Therefore, this project reflects real-world data engineering practices and strengthened my understanding of dimensional modeling, ETL pipelines, and performance optimization.