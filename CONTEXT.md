# Retail Data Pipeline

An end-to-end learning pipeline for a single e-commerce business (modelled on the Olist dataset). It turns raw operational data into clean data for analysts, dashboards, and ML models.

## Language

### Commerce

**Customer**:
A person who places Orders, identified across all their Orders (Olist's `customer_unique_id`).
_Avoid_: Buyer, client, account; never use Olist's per-order `customer_id` as a Customer

**Seller**:
A merchant on the marketplace who sells and ships Order Items; each Order Item has exactly one Seller.
_Avoid_: Vendor, merchant, supplier

**Order**:
One checkout by a Customer, containing one or more Order Items, possibly from several Sellers. Its status is the latest known value, not a history.
_Avoid_: Transaction, purchase, sale

**Order Item**:
One product line within an Order; the base grain for sales facts.
_Avoid_: Order line, line item, transaction

**Revenue**:
The sum of Order Item prices, excluding Freight, for Orders that are not canceled or unavailable.
_Avoid_: Sales, GMV, turnover, payment value

**Freight**:
The shipping charge on an Order Item, tracked separately from Revenue.
_Avoid_: Shipping fee, delivery cost

**Payment**:
One movement of money toward an Order; an Order may have several (e.g. voucher + card).
_Avoid_: Transaction

**Product**:
An item Sellers can list, described in the Merchandising Catalog; attributes reflect their current value, not their history.
_Avoid_: SKU, article

**Product Category**:
The merchandising group a Product belongs to; Products with no category belong to **Uncategorized** rather than being excluded.
_Avoid_: Department, segment

**Review**:
A Customer's 1-5 score and optional comment on one Order, given after delivery.
_Avoid_: Rating, feedback

**Late Delivery**:
A delivered Order that reached the Customer after its estimated delivery date; Orders never delivered are neither late nor on time.
_Avoid_: Delay, SLA breach

**Average Order Value**:
Revenue divided by the number of Orders counted in Revenue.
_Avoid_: AOV, basket size, ticket size

### Sources

**Shop Database**:
The operational database (Postgres) that holds Orders, Order Items, Payments, Customers, Sellers, and Reviews.
_Avoid_: OLTP, source DB, app DB

**Merchandising Catalog**:
Excel files maintained by the merchandising team, holding Product attributes and Product Category translations.
_Avoid_: Product master, reference data

### Pipeline

**Business Date**:
The calendar day (São Paulo time) that a pipeline run represents; a run for Business Date D covers Orders purchased on D.
_Avoid_: Run date, load date, processing date

**Quarantine**:
Where rows that fail Silver checks are kept, with the reason they failed, instead of being dropped or stopping the run.
_Avoid_: Error table, rejects, dead letter

**Bronze**:
An unmodified copy of source data as it arrived, tagged with when it was loaded.
_Avoid_: Raw, landing, staging

**Silver**:
Cleaned, typed, deduplicated, and conformed data, using this glossary's terms.
_Avoid_: Staging, intermediate, clean layer

**Gold**:
Business-ready tables shaped for a specific consumer (analysis, dashboard, ML feature).
_Avoid_: Marts, reporting layer
