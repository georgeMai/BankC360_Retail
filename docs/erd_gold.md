# Schema Gold Layer — Entity Relationship

## Tổng quan

Gold Layer được thiết kế theo mô hình **Wide Denormalized Table** thay vì Star Schema thuần túy. Lý do: Marketing/CSR cần query nhanh từ 1 bảng duy nhất, tránh JOIN phức tạp.

```
                          ┌─────────────────────────────────────────┐
                          │       gold.mart_customer_360             │
                          │           (MAIN TABLE)                   │
                          │                                          │
                          │  customer_id      ← PK (natural key)     │
                          │  customer_sk      ← surrogate key        │
                          │  full_name_masked                        │
                          │  age, gender                             │
                          │  primary_branch_code                     │
                          │  customer_segment, kyc_status            │
                          │  register_date                           │
                          │  ──────────────────────                  │
                          │  total_accounts                          │
                          │  total_cards                             │
                          │  total_loans                             │
                          │  has_credit_card  (INT 0/1)              │
                          │  has_savings      (INT 0/1)              │
                          │  has_loan         (INT 0/1)              │
                          │  ──────────────────────                  │
                          │  total_deposit_balance                   │
                          │  total_loan_outstanding                  │
                          │  aum_total                               │
                          │  aum_bucket                              │
                          │  ──────────────────────                  │
                          │  txn_count_30d                           │
                          │  txn_amount_30d                          │
                          │  last_txn_date    (TIMESTAMP)            │
                          │  days_since_last_txn                     │
                          │  primary_channel                         │
                          │  ──────────────────────                  │
                          │  interaction_count_90d                   │
                          │  last_interaction_date (TIMESTAMP)       │
                          │  ──────────────────────                  │
                          │  rfm_recency_score (1-5)                 │
                          │  rfm_frequency_score (1-5)               │
                          │  rfm_monetary_score (1-5)                │
                          │  rfm_segment                             │
                          │  ──────────────────────                  │
                          │  churn_flag               (INT 0/1)      │
                          │  cross_sell_credit_card_flag (INT 0/1)   │
                          └─────────────────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          ▼                          ▼
┌─────────────────────────┐  ┌────────────────────────┐  ┌─────────────────────────┐
│ gold.customer_balance   │  │  gold.customer_card    │  │ gold.customer_product   │
│ _summary                │  │  _summary              │  │ _summary                │
│                         │  │                        │  │                         │
│ customer_id FK          │  │ customer_id FK         │  │ customer_id FK          │
│ customer_sk             │  │ customer_sk            │  │ customer_sk             │
│ total_account_balance   │  │ total_cards            │  │ total_accounts          │
│ avg_account_balance     │  │ cnt_credit_active      │  │ cnt_casa_active         │
│ total_deposit_principal │  │ cnt_debit_active       │  │ cnt_td_active           │
│ total_loan_outstanding  │  │ max_credit_limit       │  │ cnt_deposits            │
│ aum_total               │  │ total_card_txn_count_  │  │ cnt_deposits_active     │
│ net_worth               │  │   30d                  │  │ cnt_loans               │
└─────────────────────────┘  │ total_card_txn_amount_ │  │ cnt_loans_active        │
                             │   30d                  │  │ cnt_cards               │
              │              │ avg_card_txn_amount_30d│  │ cnt_credit_cards        │
              ▼              │ distinct_merchant_      │  │ cnt_debit_cards         │
┌─────────────────────────────────┐ │   categories    │  │ has_credit_card (INT)   │
│ gold.customer_transaction       │  │ last_card_txn_  │  │ has_savings (INT)       │
│ _summary                        │  │   date          │  │ has_loan (INT)          │
│                                 │  └────────────────────────┘  └─────────────────────────┘
│ customer_id FK                  │
│ customer_sk                     │
│ acct_txn_count_30d              │
│ acct_txn_amount_30d             │
│ acct_credit_count_30d           │
│ acct_debit_count_30d            │
│ acct_credit_amount_30d          │
│ acct_debit_amount_30d           │
│ card_txn_count_30d              │
│ card_txn_amount_30d             │
│ total_txn_count_30d             │
│ total_txn_amount_30d            │
│ last_txn_date (TIMESTAMP)       │
└─────────────────────────────────┘
```

---

## Mô tả nghiệp vụ — Customer Summary Tables

### `gold.mart_customer_360` — Bảng chính (Main Mart)

**Mục đích**: 1 row = 1 khách hàng, tổng hợp toàn bộ 28 KPI từ tất cả nguồn. Đây là bảng duy nhất Marketing/CSR cần để query — không cần JOIN.

**Tính từ**: `silver.dim_customer` + `dim_account` + `dim_card` + `dim_loan` + `dim_deposit` + `fact_txn_account` + `fact_card_txn` + `fact_crm_interaction` + RFM scores + churn/cross-sell flags.

**Khai thác nghiệp vụ**:
- **Campaign targeting**: Lọc danh sách khách theo segment + AUM + hành vi giao dịch (ví dụ: cross-sell thẻ tín dụng Q4)
- **Customer health check**: Kết hợp `churn_flag` + `days_since_last_txn` để phát hiện khách sắp rời bỏ
- **CSR lookup**: Tra cứu nhanh hồ sơ 360° khi nhân viên đang nói chuyện với khách

**Partition**: theo `cob_dt` — giữ lịch sử snapshot theo ngày, so sánh được trạng thái khách hàng qua các tháng.

---

### `gold.customer_balance_summary` — Tổng hợp tài sản

**Mục đích**: Chi tiết tài chính của từng khách: tiền gửi, dư nợ vay, AUM. Là nguồn tính `aum_total` và `net_worth` trong mart_customer_360.

**Tính từ**: `silver.dim_account` (balance CASA + time deposit) + `silver.dim_deposit` (gốc tiết kiệm) + `silver.dim_loan` (dư nợ hiện tại).

**Khai thác nghiệp vụ**:
- **AUM segmentation**: Phân loại khách theo tổng tài sản (MASS / AFFLUENT / PRIORITY / VIP) để ưu tiên phục vụ
- **Risk management**: `net_worth` âm hoặc `total_loan_outstanding` quá cao → cảnh báo rủi ro tín dụng
- **Branch performance**: Tổng AUM theo chi nhánh để đánh giá hiệu quả huy động vốn

---

### `gold.customer_card_summary` — Tổng hợp thẻ

**Mục đích**: Tổng hợp danh mục thẻ và hành vi chi tiêu thẻ của từng khách trong 30 ngày gần nhất.

**Tính từ**: `silver.dim_card` (danh mục thẻ) + `silver.fact_card_txn` (giao dịch thẻ 30 ngày).

**Khai thác nghiệp vụ**:
- **Cross-sell thẻ tín dụng**: `cnt_credit_active = 0` → khách chưa có thẻ credit → target cross-sell
- **Spending behavior**: `avg_card_txn_amount_30d` + `distinct_merchant_categories` → hiểu thói quen chi tiêu để cá nhân hóa ưu đãi
- **Card activation**: Khách có thẻ nhưng `total_card_txn_count_30d = 0` → thẻ ngủ đông → cần chiến dịch kích hoạt

---

### `gold.customer_product_summary` — Tổng hợp sản phẩm sở hữu

**Mục đích**: Bức tranh đầy đủ về sản phẩm một khách đang giữ — tài khoản, tiết kiệm, vay, thẻ. Nguồn cho các flag `has_*` trong mart_customer_360.

**Tính từ**: `silver.dim_account` + `silver.dim_deposit` + `silver.dim_loan` + `silver.dim_card`.

**Khai thác nghiệp vụ**:
- **Product penetration**: Tỷ lệ khách có từng sản phẩm theo segment — phát hiện cơ hội upsell (ví dụ: PRIORITY có CASA nhưng chưa có tiết kiệm)
- **Cross-sell prioritization**: `no_credit_card=1 AND no_loan=1` → khách "trắng" sản phẩm tín dụng → ưu tiên tiếp cận
- **Churn indicator**: Khách chỉ còn 1 sản phẩm (`total_accounts=1, total_cards=0, total_loans=0`) → nguy cơ đóng tài khoản cao

---

### `gold.customer_transaction_summary` — Hành vi giao dịch 30 ngày

**Mục đích**: Tổng hợp tất cả giao dịch (tài khoản + thẻ) của từng khách trong cửa sổ 30 ngày gần nhất.

**Tính từ**: `silver.fact_txn_account` + `silver.fact_card_txn` (cả hai, cửa sổ 30 ngày trước `cob_dt`).

**Khai thác nghiệp vụ**:
- **Activity scoring**: `total_txn_count_30d` là input chính cho `f_score` trong RFM — khách giao dịch nhiều = loyal
- **Revenue estimation**: `acct_credit_amount_30d` (tiền vào) cho thấy dòng tiền thực tế của khách
- **Channel preference**: Kết hợp với `primary_channel` trong mart → cá nhân hóa kênh tiếp thị (mobile vs branch)
- **Anomaly detection**: Spike đột biến trong `acct_debit_amount_30d` → có thể gian lận hoặc rút tiền hàng loạt

---

## Segmentation Tables

```
                          ┌────────────────────────────┐
                          │   gold.rfm_segment          │
                          │                             │
                          │ customer_id  ← FK           │
                          │ customer_sk                 │
                          │ recency_days                │
                          │ frequency                   │
                          │ monetary                    │
                          │ r_score (1-5)               │
                          │ f_score (1-5)               │
                          │ m_score (1-5)               │
                          │ rfm_score (r+f+m, 3-15)     │
                          │ rfm_segment                 │
                          └─────────────┬───────────────┘
                                        │
                   ┌────────────────────┼────────────────────┐
                   ▼                    ▼                    ▼
    ┌──────────────────────┐  ┌──────────────────┐  ┌──────────────────────┐
    │ gold.churn_prediction│  │gold.cross_sell_  │  │gold.campaign_target  │
    │                      │  │segment           │  │                      │
    │ customer_id          │  │                  │  │ customer_id          │
    │ customer_sk          │  │ customer_id      │  │ customer_sk          │
    │ txn_cnt_30d          │  │ customer_sk      │  │ rfm_segment          │
    │ txn_cnt_90d          │  │ customer_segment │  │ rfm_score            │
    │ txn_amt_30d          │  │ no_credit_card   │  │ recency_days         │
    │ txn_amt_90d          │  │ no_deposit       │  │ frequency            │
    │ days_since_last_txn  │  │ no_loan          │  │ monetary             │
    │ churn_risk           │  │ primary_         │  │ churn_risk           │
    │ is_churn_candidate   │  │   opportunity    │  │ is_churn_candidate   │
    └──────────────────────┘  └──────────────────┘  │ days_since_last_txn  │
                                                     │ customer_segment     │
                                                     │ aum_total            │
                                                     │ aum_bucket           │
                                                     │ primary_branch_code  │
                                                     │ primary_opportunity  │
                                                     │ no_credit_card       │
                                                     │ no_deposit           │
                                                     │ no_loan              │
                                                     │ campaign_type        │
                                                     └──────────────────────┘
```

---

## Mô tả nghiệp vụ — Segmentation Tables

### `gold.rfm_segment` — Phân khúc RFM

**Mục đích**: Tính điểm RFM (Recency–Frequency–Monetary) cho từng khách hàng trên cửa sổ **90 ngày**, phân loại thành 7 nhóm hành vi từ Champions đến Lost.

**Tính từ**: `silver.fact_txn_account` + `silver.fact_card_txn` (90 ngày trước `cob_dt`).

**Khai thác nghiệp vụ**:
- **Campaign segmentation**: Champions + Loyal Customers → cross-sell cao cấp; At Risk → retention campaign; Lost → win-back
- **Priority scoring**: `rfm_score` (3–15) dùng để xếp hạng ưu tiên liên hệ trong cùng một segment
- **Trend monitoring**: So sánh `rfm_segment` qua các tháng (`cob_dt`) → phát hiện khách xuống hạng sớm

> **Lưu ý**: Bảng này tính trên 90 ngày. `mart_customer_360` nhúng RFM tính lại trên 30 ngày — hai giá trị có thể khác nhau.

---

### `gold.churn_prediction` — Dự báo rời bỏ

**Mục đích**: Gắn nhãn `is_churn_candidate` cho từng khách dựa trên quy tắc hành vi: không giao dịch trong 90 ngày, giảm tần suất, giảm số dư.

**Tính từ**: `gold.customer_transaction_summary` + `gold.customer_balance_summary`.

**Khai thác nghiệp vụ**:
- **Retention campaign**: Lọc `is_churn_candidate=1 AND aum_total > 100M` → danh sách khách giá trị cao cần giữ chân ngay
- **Early warning**: `churn_risk` score liên tục — theo dõi hàng tuần để can thiệp trước khi khách đóng tài khoản
- **Branch accountability**: Tổng hợp `is_churn_candidate` theo chi nhánh → đánh giá hiệu quả chăm sóc khách hàng

---

### `gold.cross_sell_segment` — Cơ hội bán chéo

**Mục đích**: Xác định sản phẩm nào khách chưa có (`no_credit_card`, `no_deposit`, `no_loan`) và tổng hợp cơ hội bán chéo chính (`primary_opportunity`).

**Tính từ**: `gold.customer_product_summary` + `silver.dim_customer`.

**Khai thác nghiệp vụ**:
- **Product gap analysis**: Khách PRIORITY có CASA nhưng `no_deposit=1` → tiềm năng mở sổ tiết kiệm
- **Revenue forecasting**: Ước tính revenue tiềm năng nếu đẩy được sản phẩm còn thiếu vào từng nhóm khách
- **Sales team targeting**: Mỗi banker được cấp danh sách khách theo `primary_opportunity` phù hợp với KPI của họ

---

### `gold.campaign_target` — Danh sách mục tiêu chiến dịch

**Mục đích**: Bảng tổng hợp cuối cùng cho Marketing — kết hợp RFM + churn risk + cross-sell + AUM + địa lý thành 1 view sẵn sàng export cho chiến dịch.

**Tính từ**: `gold.rfm_segment` + `gold.churn_prediction` + `gold.cross_sell_segment` + `gold.customer_balance_summary`.

**Khai thác nghiệp vụ**:
- **Campaign execution**: Marketing filter theo `campaign_type` (CROSS_SELL / RETENTION / UPSELL) → xuất danh sách gọi điện/gửi SMS ngay
- **Priority queue**: Sắp xếp theo `aum_total DESC` trong cùng `campaign_type` → telesales gọi khách giá trị cao trước
- **Multi-wave targeting**: `rfm_score` + `churn_risk` kết hợp để chia đợt gửi: đợt 1 Champions, đợt 2 Loyal Customers, đợt 3 At Risk

---

## Analytics Table

```
┌────────────────────────────────────────┐
│  gold.mart_branch_monthly_summary      │
│                                        │
│  branch_code                           │
│  branch_name                           │
│  region, city                          │
│  txn_year, txn_month, txn_quarter      │
│  active_customers                      │
│  txn_count                             │
│  total_txn_amount                      │
│  avg_txn_amount                        │
│  total_credit_amount                   │
│  total_debit_amount                    │
│  top_channel                           │
└────────────────────────────────────────┘
```

---

## Mô tả nghiệp vụ — Analytics Table

### `gold.mart_branch_monthly_summary` — Hiệu quả chi nhánh theo tháng

**Mục đích**: Tổng hợp hoạt động giao dịch của từng chi nhánh theo tháng/quý — bảng duy nhất phục vụ báo cáo quản lý cấp chi nhánh và vùng.

**Tính từ**: `silver.fact_txn_account` JOIN `silver.dim_account` (để lấy `branch_code`) GROUP BY branch + tháng.

**Khai thác nghiệp vụ**:
- **Branch ranking**: So sánh `total_txn_amount` và `active_customers` giữa các chi nhánh trong cùng `region` → xếp hạng hiệu quả kinh doanh
- **Seasonal analysis**: `txn_quarter` cho phép so sánh Q4 vs Q3 → đo lường impact của chiến dịch cuối năm
- **Channel shift**: `top_channel` theo tháng → theo dõi xu hướng dịch chuyển từ BRANCH sang MOBILE_BANKING theo thời gian
- **Capacity planning**: `active_customers` tăng trưởng theo tháng → dự báo nhu cầu nhân sự chi nhánh

---

## Bảng gold.mart_customer_360 — Column Reference

### Đầy đủ danh sách columns theo thứ tự

```sql
-- DDL reference (Spark/Iceberg syntax)
CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_customer_360 (
    -- Identity
    customer_id                 BIGINT,         -- ID từ Oracle source
    customer_sk                 STRING,         -- SHA256 surrogate key (từ SCD2)
    full_name_masked            STRING,         -- Họ tên đã mask

    -- Profile
    age                         INT,
    gender                      STRING,
    primary_branch_code         STRING,
    customer_segment            STRING,         -- RETAIL/PRIORITY/VIP
    kyc_status                  STRING,
    register_date               DATE,

    -- Product Holding
    total_accounts              INT,
    total_cards                 INT,
    total_loans                 INT,
    has_credit_card             INT,            -- 0 hoặc 1
    has_savings                 INT,            -- 0 hoặc 1
    has_loan                    INT,            -- 0 hoặc 1

    -- Balance & AUM
    total_deposit_balance       DECIMAL(18,2),
    total_loan_outstanding      DECIMAL(18,2),
    aum_total                   DECIMAL(18,2),
    aum_bucket                  STRING,         -- MASS/AFFLUENT/PRIORITY/VIP

    -- Transaction Behavior (30 ngày gần nhất)
    txn_count_30d               BIGINT,
    txn_amount_30d              DECIMAL(18,2),
    last_txn_date               TIMESTAMP,
    days_since_last_txn         INT,
    primary_channel             STRING,

    -- CRM Engagement
    interaction_count_90d       BIGINT,
    last_interaction_date       TIMESTAMP,

    -- RFM Scoring (NTILE 5, tính trên 30 ngày)
    rfm_recency_score           INT,            -- 1-5
    rfm_frequency_score         INT,            -- 1-5
    rfm_monetary_score          INT,            -- 1-5
    rfm_segment                 STRING,

    -- Risk Flags (0/1)
    churn_flag                  INT,
    cross_sell_credit_card_flag INT,

    -- Partition
    cob_dt                      DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt));
```

---

## Quan hệ với Silver Layer

Gold Layer được build từ Silver Layer. Không có foreign key vật lý trong Iceberg — đây là **logical relationships**.

```
silver.dim_customer (is_current=1)
    customer_id ──────────────────────────► gold.mart_customer_360.customer_id
    customer_sk
    full_name, gender, date_of_birth, branch_code

silver.dim_account (is_current=1)
    customer_id ──► customer_id          ──► total_accounts, has_savings

silver.dim_card (is_current=1)
    customer_id ──► customer_id          ──► total_cards, has_credit_card

silver.dim_loan
    customer_id ──► customer_id          ──► total_loans, has_loan, total_loan_outstanding

silver.dim_deposit
    customer_id ──► customer_id          ──► total_deposit_balance

silver.fact_txn_account + silver.fact_card_txn  (30 ngày)
    customer_id ──► customer_id          ──► txn_count_30d, txn_amount_30d, last_txn_date

silver.fact_crm_interaction (90 ngày)
    customer_id ──► customer_id          ──► interaction_count_90d, last_interaction_date
```

---

## AUM Bucket Thresholds

| Bucket | Ngưỡng AUM | Đối tượng |
|--------|-----------|----------|
| `MASS` | < 100,000,000 VND | Khách hàng phổ thông |
| `AFFLUENT` | 100,000,000 – 1,000,000,000 VND | Khách hàng khá giả |
| `PRIORITY` | 1,000,000,000 – 5,000,000,000 VND | Khách hàng ưu tiên |
| `VIP` | ≥ 5,000,000,000 VND | Khách hàng VIP |

---

## RFM Segment Matrix

Segment được xác định bằng **tổng R+F+M score** (mỗi score từ 1–5, tổng từ 3–15):

| Segment | Tổng RFM | Mô tả |
|---------|---------|-------|
| Champions | ≥ 13 | Khách hàng tốt nhất, giao dịch nhiều và gần đây |
| Loyal Customers | ≥ 10 | Trung thành, giá trị cao |
| Potential Loyalists | ≥ 7 | Tiềm năng tốt, cần nuôi dưỡng |
| New Customers | ≥ 5 | Mới join, chưa quen |
| At Risk | ≥ 3 | Giá trị cao nhưng đang giảm hoạt động |
| Hibernating | ≥ 2 | Không hoạt động |
| Lost | < 2 | Mất liên lạc hoàn toàn |

> **Sandbox view vs Gold table**: `gold.rfm_segment` tính RFM trên **90 ngày** với các cột raw (`r_score`, `f_score`, `m_score`). `gold.mart_customer_360` nhúng điểm RFM tính lại trên **30 ngày** cho tiện query. Hai giá trị có thể khác nhau.
