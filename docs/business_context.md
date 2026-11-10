# Bối cảnh Nghiệp vụ — Customer 360 Lakehouse

## 1. Vấn đề hiện tại (Before)

### Mô hình dữ liệu hiện tại của ngân hàng bán lẻ

Một ngân hàng bán lẻ điển hình có dữ liệu nằm rải rác ở nhiều hệ thống:

| Hệ thống | Dữ liệu | Công nghệ |
|----------|---------|-----------|
| Core Banking | Tài khoản, tiền gửi, khoản vay, giao dịch | Oracle |
| Card Management | Thẻ debit/credit, giao dịch thẻ | PostgreSQL |
| CRM | Tương tác khách hàng, campaigns | PostgreSQL |

**Vấn đề**: Không có cái nhìn thống nhất về khách hàng. Mỗi câu hỏi phân tích đòi hỏi DBA phải viết query JOIN qua nhiều hệ thống, mất 2–3 ngày, và kết quả không reproducible.

### Pain points cụ thể

1. **Marketing không tự phục vụ được**: Muốn lấy danh sách 1.000 khách tiềm năng cho chiến dịch → phải chờ DBA → deadline bị trễ.
2. **Không có lịch sử thay đổi**: Khách đổi địa chỉ, thay đổi segment → dữ liệu cũ bị ghi đè, mất audit trail.
3. **Không có KPI chuẩn**: Mỗi team tính "tổng tiền gửi" theo cách khác nhau → số liệu không nhất quán trong báo cáo.
4. **Query chậm trên production**: Chạy báo cáo trực tiếp trên Oracle Core Banking → ảnh hưởng hiệu năng hệ thống giao dịch.

---

## 2. Giải pháp — Customer 360 Lakehouse

### Tầm nhìn

**1 row / 1 khách hàng** trong bảng `mart_customer_360` chứa đầy đủ 28 KPIs để trả lời mọi câu hỏi phân tích thường gặp, mà Marketing/CSR có thể tự query mà không cần DBA.

### Kết quả đạt được (After)

| Chỉ tiêu | Trước Lakehouse | Sau Lakehouse |
|----------|-----------------|---------------|
| Thời gian lấy danh sách khách hàng | 2–3 ngày | < 10 giây |
| Người thực hiện query | DBA | Marketing tự query |
| Lịch sử thay đổi khách hàng | Không có | SCD Type 2, đầy đủ |
| Nguồn dữ liệu nhất quán | Không | 1 source of truth (Silver layer) |
| Tác động lên production DB | Có (query trực tiếp) | Không (tách biệt Lakehouse) |

---

## 3. Demo Story — Chiến dịch Cross-sell Thẻ Tín Dụng Q4

### Bối cảnh

Cuối tháng 12/2025, team Marketing muốn chạy chiến dịch **"Cross-sell thẻ tín dụng Q4"** nhắm vào khách hàng tiềm năng nhất ở TP.HCM.

### Tiêu chí lọc

| Tiêu chí | Lý do nghiệp vụ |
|----------|-----------------|
| Chưa có thẻ tín dụng | Đây là sản phẩm cần cross-sell |
| Tổng tiền gửi > 100 triệu VND | Khả năng tài chính đủ để trả nợ thẻ |
| RFM Segment = Champions hoặc Loyal Customers | Khách hàng trung thành, ít rủi ro |
| Có giao dịch trong 30 ngày gần nhất | Khách đang active, không bỏ ngân hàng |
| Chi nhánh khu vực HCM | Ưu tiên theo vùng địa lý cho chiến dịch |

### Query thực tế (chạy trên Trino, < 10 giây)

```sql
-- File: sql_templates/trino/01_cross_sell_credit_card.sql
SELECT
    customer_id,
    full_name_masked,
    primary_branch_code,
    aum_total,
    aum_bucket,
    rfm_segment,
    txn_count_30d,
    last_txn_date
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE has_credit_card = 0
  AND aum_total > 100000000
  AND rfm_segment IN ('Champions', 'Loyal Customers')
  AND days_since_last_txn <= 30
  AND primary_branch_code LIKE 'HCM%'
ORDER BY aum_total DESC
LIMIT 1000;
```

### Kết quả mong đợi với dữ liệu demo

- ~400–600 khách hàng Priority/VIP ở HCM chưa có thẻ tín dụng
- Được sắp xếp theo tổng tài sản giảm dần
- Dữ liệu đã mask PII (full_name_masked, không hiển thị CCCD)

---

## 4. Stakeholders

| Role | Người dùng | Nhu cầu |
|------|-----------|---------|
| **Primary** | Team CRM/Marketing | Tự query danh sách khách hàng cho chiến dịch |
| **Secondary** | Team CSR (Customer Service) | Tra cứu hồ sơ 360° của khách hàng khi gọi điện |
| **Secondary** | Team Risk | Phân tích churn risk, khách hàng inactive |
| **Builder** | Big Data Engineer | Xây dựng và vận hành pipeline |

---

## 5. Use Cases được hỗ trợ (6 Trino Queries)

### UC1: Cross-sell thẻ tín dụng
**File**: `sql_templates/trino/01_cross_sell_credit_card.sql`
**Owner**: Marketing Team
**Mô tả**: Lọc khách hàng tiềm năng chưa có thẻ tín dụng, có đủ tiền gửi và đang active.

---

### UC2: Churn Risk Detection
**File**: `sql_templates/trino/02_churn_risk_customers.sql`
**Owner**: Retention Team
**Mô tả**: Tìm khách hàng không có giao dịch trong 90 ngày gần nhất, đặc biệt những người trước đây là high-value.

```sql
-- Khách hàng có nguy cơ churn: inactive > 90 ngày nhưng AUM > 100 triệu
SELECT customer_id, aum_total, days_since_last_txn, rfm_segment
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE churn_flag = 1
  AND aum_total > 100000000
ORDER BY aum_total DESC;
```

---

### UC3: RFM Segment Distribution
**File**: `sql_templates/trino/03_rfm_segment_distribution.sql`
**Owner**: Analytics Team
**Mô tả**: Báo cáo phân bổ khách hàng theo RFM segment — dùng để đánh giá chất lượng khách hàng theo tháng.

---

### UC4: AUM Breakdown by Branch
**File**: `sql_templates/trino/04_aum_breakdown_by_branch.sql`
**Owner**: Branch Management
**Mô tả**: Tổng tài sản khách hàng (AUM) phân theo chi nhánh — đo hiệu quả kinh doanh từng chi nhánh.

---

### UC5: Customer 360 Profile
**File**: `sql_templates/trino/05_customer_360_profile.sql`
**Owner**: CSR Team
**Mô tả**: Hồ sơ đầy đủ 1 khách hàng: tất cả tài khoản, thẻ, vay, giao dịch gần nhất, lịch sử tương tác.

---

### UC6: Product Penetration Analysis
**File**: `sql_templates/trino/06_product_penetration_analysis.sql`
**Owner**: Product Team
**Mô tả**: Tỷ lệ sở hữu từng sản phẩm (CASA, tiết kiệm, vay, thẻ) theo segment khách hàng — phát hiện cơ hội upsell.

---

## 6. Dữ liệu Giả lập — Đặc điểm quan trọng

Dữ liệu được sinh theo phân bố thực tế để demo story chạy được:

### Phân bổ khách hàng

| Segment | Số lượng | % |
|---------|---------|---|
| RETAIL | 7,000 | 70% |
| PRIORITY | 2,500 | 25% |
| VIP | 500 | 5% |

### Tỷ lệ sở hữu thẻ tín dụng (quan trọng cho cross-sell demo)

| Segment | Có thẻ tín dụng | Chưa có (target cross-sell) |
|---------|----------------|----------------------------|
| VIP | ~350 (70%) | **~150 (30%)** |
| PRIORITY | ~1,250 (50%) | **~1,250 (50%)** |
| RETAIL | ~1,050 (15%) | ~5,950 (85%) |

→ Có ~1,400 khách Priority + VIP chưa có thẻ tín dụng — đây là target pool cho chiến dịch cross-sell.

> **Dataset**: 12 tháng lịch sử giao dịch (Jan–Dec 2025), ~1,050,000 txn tài khoản và ~212,000 txn thẻ. Đủ để churn detection 365 ngày và RFM 90 ngày có ý nghĩa thống kê.

### Phân bổ địa lý

| Thành phố | % |
|-----------|---|
| TP.HCM | 35% |
| Hà Nội | 25% |
| Đà Nẵng | 8% |
| Hải Phòng | 5% |
| Tỉnh khác | 27% |

### Phân bổ AUM (Pareto)

| Segment | AUM trung bình |
|---------|----------------|
| VIP | 1–10 tỷ VND |
| PRIORITY | 100 triệu – 1 tỷ VND |
| RETAIL | 1–100 triệu VND |
