# Từ điển KPI — Customer 360 (`mart_customer_360`)

Bảng `gold.mart_customer_360` là bảng trung tâm của Lakehouse, chứa **1 row per khách hàng** với các KPIs được tính sẵn. Marketing, CSR, và Risk có thể query trực tiếp bằng Trino mà không cần JOIN.

> **Truy cập qua Trino**: Dùng schema `lakehouse.sandbox.mart_customer_360_masked` cho dữ liệu đã mask PII (Marketing/CSR), hoặc `lakehouse.gold.mart_customer_360` cho dữ liệu đầy đủ (nội bộ).

---

## Nhóm 1: Identity & Profile

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `customer_id` | BIGINT | ID khách hàng từ Oracle Core Banking. Khóa chính để JOIN qua tất cả các bảng. | `79200012345` |
| `customer_sk` | STRING | Surrogate key (SHA256), sinh ra bởi SCD2 logic. Bất biến theo thời gian. | `a3f8b2...` |
| `full_name_masked` | STRING | Họ tên đã mask PII. Chỉ hiển thị họ, tên đệm và tên bị che. | `Nguyễn V** A**` |
| `age` | INT | Tuổi tính theo năm, tính từ `date_of_birth` đến `cob_dt`. | `42` |
| `gender` | STRING | Giới tính: `Male` / `Female` / `Other`. | `Male` |
| `primary_branch_code` | STRING | Mã chi nhánh mở tài khoản đầu tiên. Dùng để phân tích theo vùng địa lý. | `HCM010` |
| `customer_segment` | STRING | Phân khúc khách hàng: `RETAIL` / `PRIORITY` / `VIP`. | `PRIORITY` |
| `kyc_status` | STRING | Trạng thái KYC: `VERIFIED` / `PENDING` / `REJECTED`. | `VERIFIED` |
| `register_date` | DATE | Ngày đăng ký tài khoản lần đầu. | `2020-03-15` |

---

## Nhóm 2: Product Holding

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `total_accounts` | INT | Tổng số tài khoản (CASA + TIME_DEPOSIT). | `3` |
| `total_cards` | INT | Tổng số thẻ (debit + credit + prepaid). | `2` |
| `total_loans` | INT | Tổng số khoản vay. | `1` |
| `has_credit_card` | INT | `1` nếu khách có ít nhất 1 thẻ tín dụng ACTIVE. **KPI quan trọng nhất cho cross-sell.** | `0` |
| `has_savings` | INT | `1` nếu có ít nhất 1 sổ tiết kiệm (deposit). | `1` |
| `has_loan` | INT | `1` nếu có ít nhất 1 khoản vay. | `0` |

> **Lưu ý**: Các flag được lưu dưới dạng INT (0/1), không phải BOOLEAN. Khi filter: `WHERE has_credit_card = 0`.

---

## Nhóm 3: Balance & AUM

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `total_deposit_balance` | DECIMAL(18,2) | Tổng số dư tài khoản active + gốc tiết kiệm active. Đơn vị: VND. | `250,000,000` |
| `total_loan_outstanding` | DECIMAL(18,2) | Tổng dư nợ khoản vay. Đơn vị: VND. | `150,000,000` |
| `aum_total` | DECIMAL(18,2) | AUM = `total_deposit_balance`. Dùng để xếp loại khách hàng. | `250,000,000` |
| `aum_bucket` | STRING | Phân loại AUM: `MASS` (< 100tr), `AFFLUENT` (100tr–1 tỷ), `PRIORITY` (1–5 tỷ), `VIP` (> 5 tỷ). | `AFFLUENT` |

**Lưu ý**: `aum_total` hiện chỉ tính tiền gửi (account balance + deposit principal). Trong production, AUM cần tính thêm tài sản đầu tư, bảo hiểm, v.v.

---

## Nhóm 4: Transaction Behavior

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `txn_count_30d` | BIGINT | Tổng số giao dịch (account + card) trong 30 ngày gần nhất. | `45` |
| `txn_amount_30d` | DECIMAL(18,2) | Tổng số tiền giao dịch trong 30 ngày gần nhất. Đơn vị: VND. | `32,500,000` |
| `last_txn_date` | TIMESTAMP | Timestamp giao dịch cuối cùng (account hoặc card). | `2024-12-28 14:30:00` |
| `days_since_last_txn` | INT | Số ngày từ giao dịch cuối đến `cob_dt`. Giá trị cao → khách có thể churn. | `2` |
| `primary_channel` | STRING | Kênh giao dịch nhiều nhất trong 30 ngày: `MOBILE_BANKING`, `INTERNET_BANKING`, `ATM`, `POS`, `BRANCH`. | `MOBILE_BANKING` |

---

## Nhóm 5: CRM Engagement

| Column | Type | Mô tả | Ví dụ |
|--------|------|-------|-------|
| `interaction_count_90d` | BIGINT | Số lần tương tác CRM trong 90 ngày gần nhất (call, email, chat, branch, SMS). | `3` |
| `last_interaction_date` | TIMESTAMP | Timestamp tương tác CRM gần nhất. Null nếu chưa có tương tác. | `2024-12-15 09:00:00` |

---

## Nhóm 6: RFM Score & Segment

RFM (Recency, Frequency, Monetary) là phương pháp phân khúc khách hàng phổ biến. Điểm được tính bằng NTILE(5) trên toàn bộ khách hàng.

| Column | Type | Mô tả |
|--------|------|-------|
| `rfm_recency_score` | INT (1–5) | Điểm Recency. 5 = giao dịch rất gần đây. |
| `rfm_frequency_score` | INT (1–5) | Điểm Frequency. 5 = giao dịch rất thường xuyên. |
| `rfm_monetary_score` | INT (1–5) | Điểm Monetary. 5 = chi tiêu rất cao. |

### RFM Segment (dựa trên tổng R+F+M score, max=15)

| Segment | Tổng RFM | Hành động |
|---------|----------|----------|
| `Champions` | ≥ 13 | Reward, upsell premium products |
| `Loyal Customers` | ≥ 10 | Loyalty program, cross-sell |
| `Potential Loyalists` | ≥ 7 | Tăng tần suất tương tác |
| `New Customers` | ≥ 5 | Onboarding, activation |
| `At Risk` | ≥ 3 | Win-back campaigns, ưu đãi đặc biệt |
| `Hibernating` | ≥ 2 | Reactivation campaign |
| `Lost` | < 2 | Không đầu tư thêm hoặc thử farewell offer |

> **Lưu ý**: `gold.rfm_segment` (bảng riêng) dùng cùng logic nhưng tính trên 90 ngày thay vì 30 ngày, và lưu thêm các cột raw: `r_score`, `f_score`, `m_score`, `rfm_score`.

---

## Nhóm 7: Risk Flags

| Column | Type | Mô tả | Điều kiện |
|--------|------|-------|-----------|
| `churn_flag` | INT | `1` nếu khách có khả năng rời bỏ ngân hàng. | `days_since_last_txn > 90` hoặc `last_txn_date IS NULL` |
| `cross_sell_credit_card_flag` | INT | `1` nếu khách đủ điều kiện để cross-sell thẻ tín dụng. | `aum_total >= 100,000,000 AND has_credit_card = 0` |

---

## Cách dùng mart_customer_360

### Query 1: Lọc khách hàng cho chiến dịch cross-sell

```sql
-- Dùng bảng masked cho Marketing (không lộ PII)
SELECT customer_id, full_name_masked, aum_total, rfm_segment, primary_branch_code
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE cross_sell_credit_card_flag = 1
  AND days_since_last_txn <= 30
ORDER BY aum_total DESC
LIMIT 1000;
```

### Query 2: Dashboard tổng quan portfolio

```sql
-- Số lượng khách theo segment và AUM bucket
SELECT rfm_segment, aum_bucket, COUNT(*) as customer_count, SUM(aum_total) as total_aum
FROM lakehouse.gold.mart_customer_360
GROUP BY rfm_segment, aum_bucket
ORDER BY total_aum DESC;
```

### Query 3: Hồ sơ khách hàng cụ thể (CSR tra cứu)

```sql
-- Dùng customer_id từ hệ thống CRM
SELECT *
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE customer_id = 79200012345;
```

---

## Metadata Columns

| Column | Mô tả |
|--------|-------|
| `cob_dt` | Close-of-Business date (ngày dữ liệu), dùng làm partition column |
