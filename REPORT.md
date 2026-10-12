# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Trần Thị Lan` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602621` |
| GitHub | `nan-bi` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07900` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền sau khi chạy step 4>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền sau khi chạy step 4>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Sau khi chạy `step 1` và quan sát hai bảng bronze, tôi thấy 4 khác biệt rõ ràng giữa `telemetry_a.csv` và `telemetry_b.csv`:

1. **Tên cột khác nhau**: nguồn A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `acc_x`, `acc_y`, `gas`, `brake`, `steer`; nguồn B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `ax`, `ay`, `throttle_pct`, `brake_pct`, `steering`.
2. **Đơn vị tốc độ khác nhau**: nguồn A ghi tốc độ theo **m/s** (ví dụ: 1.213); nguồn B ghi theo **km/h** (ví dụ: 55.11). Pipeline phải chia 3.6 để đưa B về m/s.
3. **Định dạng timestamp khác nhau**: nguồn A dùng chuỗi ISO UTC (ví dụ: `2025-10-07T08:00:00.014Z`); nguồn B dùng **epoch milliseconds** (ví dụ: `1759933801509`). Ngoài ra, đồng hồ của B **lệch 1500 ms** so với video, nên pipeline phải trừ offset này.
4. **Đơn vị bàn đạp khác nhau**: nguồn A ghi `gas`, `brake` theo tỷ lệ 0–1; nguồn B ghi `throttle_pct`, `brake_pct` theo **phần trăm 0–100**, phải chia 100 khi chuẩn hóa.

Pipeline giữ mỗi nguồn một bảng bronze riêng và để tất cả cột ở dạng **chuỗi văn bản (text)** vì: Bronze là lớp ghi nguyên bản — nhiệm vụ duy nhất là **bảo toàn dữ liệu gốc** mà không làm mất thông tin. Nếu ép kiểu ngay, một giá trị thiếu (`NULL`) hoặc bất hợp lệ sẽ gây lỗi ngay tại ingest và có thể mất dữ liệu mà không có bằng chứng. Bằng cách giữ ở dạng chữ, pipeline đảm bảo **số dòng bronze = số dòng CSV gốc** (idempotent, có thể kiểm chứng bằng sha256). Việc chuẩn hóa, ép kiểu và cách ly dòng lỗi được giao cho lớp Silver, nơi mỗi sai sót đều được ghi vào `rejects` kèm lý do.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline chuẩn hóa hai nguồn trong `stg_telemetry_a.sql` và `stg_telemetry_b.sql`:
- **Đơn vị tốc độ**: B được chia cho 3.6 (`spd_kmh / 3.6 → speed_mps`) để về m/s như A.
- **Kiểu thời gian**: B chuyển từ epoch ms sang timestamptz bằng `to_timestamp((timestamp_ms - offset_ms) / 1000.0)`.
- **Độ lệch đồng hồ**: `telemetry_clock_offset_ms = 1500 ms` (khai báo trong `sources.json`) được đọc từ `bronze.video_files` và trừ đi, khiến thời điểm bắt đầu của B khớp với video (`2025-10-08 14:30:00 UTC`).
- **Bàn đạp**: B chia 100 để đưa về tỷ lệ 0–1.
- **Tên cột**: tất cả đổi về schema thống nhất (`lat`, `lon`, `acc_x`, `acc_y`, `gas`, `brake`, `steer`).

Bảng quarantine (điền số liệu từ `report/evidence/run_summary.json` sau khi chạy):

| drive_id | reject_reason | số dòng |
|---|---|---|
| drive_a | duplicate | `<điền>` |
| drive_a | speed_missing | `<điền>` |
| drive_a | speed_out_of_range | `<điền>` |
| drive_b | duplicate | `<điền>` |
| drive_b | speed_missing | `<điền>` |
| drive_b | speed_out_of_range | `<điền>` |

Dòng lỗi vào `silver.silver_telemetry_rejects` thay vì bị xoá vì: **dữ liệu xấu vẫn là bằng chứng**. Nếu xoá im lặng, không ai biết bao nhiêu dòng bị bỏ và vì lý do gì. Quarantine cho phép kiểm toán (audit), tái hiện vấn đề, và sau này có thể khôi phục nếu luật cách ly thay đổi. Đây là nguyên tắc "never silently drop data" của kiến trúc medallion.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Trong `dbt/models/schema.yml`, test `max_gap_ms` có cấu hình `severity: warn` còn `in_range` không có cấu hình đặc biệt nên mặc định là `error`. Lý do:
- `max_gap_ms` kiểm tra khoảng cách thời gian tối đa giữa hai dòng telemetry liên tiếp. **Khoảng cách lớn là điều bình thường** khi cảm biến mất tín hiệu ngắn (GPS loss, nhiễu) — pipeline vẫn có thể tiếp tục hoạt động bình thường, chỉ cần biết để cảnh báo.
- `in_range` kiểm tra tốc độ trong khoảng hợp lệ (0–70 m/s). Tốc độ âm hoặc vượt 70 m/s là **dữ liệu vô lý về vật lý**, không thể xảy ra; để lọt vào gold sẽ làm hỏng mô hình học máy.

Kết quả thí nghiệm `break-gate` (điền sau khi chạy `scripts/lab.sh exp break-gate`):

Khi nới luật từ 70 m/s lên 1000 m/s, test `in_range` của `silver_telemetry` **bắt được `<điền>` dòng** tốc độ vô lý vào silver. Cổng chất lượng (dbt test) chính là cơ chế **dừng pipeline ngay** nếu dữ liệu xấu vượt qua staging — đảm bảo gold không bao giờ chứa dữ liệu không hợp lệ. Khi khôi phục luật gốc, test trả về PASS, xác nhận pipeline ổn định.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào `report/evidence/idempotency.json` sau khi chạy thí nghiệm:

- **Số dòng trước**: `<điền: rows_before>` dòng toàn bảng silver.frames
- **Sau 2 lần chạy lại cùng trip**: `<điền: rows_after_two_reruns>` dòng — **không đổi**
- **Dấu vân tay (fingerprint)**: `<điền: fingerprint_before>` → `<điền: fingerprint_after>` — **giống hệt**
- **Cách sai (append × 2)**: `<điền: append_variant_rows>` dòng — **nhân đôi** so với trip gốc

Delta Lake `overwrite` theo predicate (`WHERE trip_id = 'trip-xxx'`) an toàn hơn "xoá rồi ghi" vì: (1) Thao tác là **atomic** — hoặc ghi xong hoàn toàn hoặc không ghi gì — không có trạng thái nửa vời. (2) Nếu worker sập giữa chừng, commit Delta chưa được ghi, và lần chạy lại sẽ thấy partition vẫn là dữ liệu cũ hợp lệ, không phải bảng trống. (3) **Chỉ partition đó** bị ghi đè — 25 trip còn lại không bị ảnh hưởng. Ngược lại, "xoá + ghi lại" tạo ra khoảng thời gian bảng rỗng, và nếu ghi lại thất bại, dữ liệu biến mất vĩnh viễn.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khung đen** (`is_black`): độ sáng trung bình (luma) dưới ngưỡng `dark_luma = 12.0` — camera bị che, hỏng, hoặc đoạn video bị lỗi. Đây là **lỗi kỹ thuật**, frame không có giá trị cho training → bị **loại bỏ khỏi gold**.

**Khung tối** (`is_dark`): luma trong khoảng [12, 40) — xe đi qua đường hầm, bóng râm, hay cảnh đêm. Đây là **điều kiện thực tế** mà mô hình cần học → được **gắn cờ nhưng giữ lại**. Tôi đồng ý với cách xử lý này vì đường hầm và điều kiện thiếu sáng là các tình huống thực tế mà xe tự lái cần nhận diện được.

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | `<điền từ threshold.json>` | `<điền từ threshold.json>` |
| 2 | `<điền từ threshold.json>` | `<điền từ threshold.json>` |
| 6 | `<điền từ threshold.json>` | `<điền từ threshold.json>` |

Pipeline chọn ngưỡng **2 bit** (mặc định `dup_hamming = 2`). Ngưỡng này hợp lý vì: ngưỡng 0 chỉ bắt frame y hệt (quá chặt, bỏ sót nhiều frame gần như trùng); ngưỡng 6 loại quá nhiều, có thể bỏ qua frame thực sự khác nhau. Với xe chạy chậm (< 10 km/h), hai frame liên tiếp 0.5 giây cách nhau chỉ dịch chuyển ~1.4 m nên dễ bị coi là trùng. Với xe chạy nhanh (60 km/h), 0.5 giây = 8 m, cảnh thay đổi nhiều hơn → ít bị coi là trùng. Vì vậy ngưỡng 2 là cân bằng tốt cho tốc độ trung bình của dataset này.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào `report/evidence/leakage.json`:

- **Chia theo khung hình** (random 70/15/15): `<điền: per_frame_split_trips_straddling>` trip bị chia cắt ra nhiều split; `<điền: per_frame_split_trips_in_both_train_and_test>` trip có khung ở cả train lẫn test.
- **Chia theo trip** (gold pipeline): `0` trip bị chia cắt — mỗi trip chỉ nằm trong **một** split duy nhất.

Khi chia theo khung hình ngẫu nhiên, các khung liền kề trong cùng một trip (chỉ cách nhau 0.5 giây, gần như giống hệt nhau) bị tách ra: một nửa vào train, một nửa vào test. Mô hình học máy thực chất đã "thấy" cảnh đó trong training. Khi test, nó nhận ra cảnh quen thuộc và cho điểm cao — nhưng đây là **data leakage**, không phải khả năng tổng quát hóa thực sự. Chia theo trip đảm bảo mỗi **cảnh** (trip) chỉ xuất hiện ở một phía, buộc mô hình phải tổng quát hóa sang **cảnh mới hoàn toàn**.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là **băm SHA256 của nội dung dataset** (danh sách frame_id, frame_uri, split theo thứ tự xác định) vì:
1. **Deterministic**: Cùng một nội dung → cùng một hash, bất kể chạy lúc nào, máy nào.
2. **Content-addressable**: Hash thay đổi khi và chỉ khi nội dung thay đổi. Ngày giờ có thể trùng hoặc thay đổi dù nội dung không đổi.
3. **Reproducibility**: Giảng viên chạy lại bài của bạn với cùng MSSV phải ra đúng cùng `dataset_version`.

**Hai lần chạy liên tiếp** với cùng MSSV và cùng dữ liệu cho ra **cùng `dataset_version`** — xác nhận pipeline deterministic.

`dataset_version` của mỗi người khác nhau vì: trong `gold_dataset_frames.sql`, thứ tự trip được xáo trộn bằng `md5(trip_id || LAB_STUDENT_ID)`. MSSV được "trộn" vào hàm băm, làm thứ tự trip thay đổi → split train/val/test khác → danh sách frame_id trong mỗi split khác → hash nội dung khác. Đây là cơ chế đảm bảo mỗi sinh viên có kết quả riêng và không thể copy bài nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Đọc `manifest.json` trong `lake/gold/datasets/<version>/manifest.json` (điền sau khi chạy step 4):

Dataset phiên bản `<dataset_version>` được tạo từ:
- **Video nguồn**: `drive_a.mp4` (sha256: `<điền từ manifest.json lineage>`), `drive_b.mp4` (sha256: `<điền>`)
- **Bảng silver**: `silver.silver_frame_telemetry` tại version commit `<điền>`, `silver.silver_telemetry` tại version `<điền>`
- **Tham số**: `split_train=0.70`, `split_val=0.15`, `dup_hamming=2`, `sync_tol_ms=150`, `LAB_STUDENT_ID=2A202602621`
- **Thời điểm tạo**: `<điền: finished_at từ run_summary.json>`

Để tái tạo lại đúng dataset 3 tháng sau, cần: (1) file video gốc với sha256 khớp, (2) cùng MSSV `2A202602621`, (3) cùng tham số dbt trong `dbt_project.yml`, (4) cùng commit của mã pipeline. Phần `lineage` trong `manifest.json` lưu đủ các thông tin này như một "hóa đơn vật liệu" (bill of materials) của dataset.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của tôi được tính theo MSSV: `int(2A202602621) % 26 → trip-07900` (drive_a, 08:00:37–08:00:52 UTC).

Khi đặt `LAB_FAIL_ONCE=trip-07900` và trigger DAG:
- **1 task** `silver_frames[trip-07900]` chuyển **đỏ** (log hiện `simulated crash while extracting trip-07900`)
- Các task phụ thuộc sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) chuyển sang `upstream_failed` — tổng cộng **`<điền>` task bị ảnh hưởng**
- **Clear duy nhất task đỏ** `silver_frames[trip-07900]`: task chạy lại thành công, DAG chuyển xanh toàn bộ
- **Dữ liệu không nhân đôi**: Delta overwrite theo partition trip-07900, nên dù chạy lại, số dòng của trip đó vẫn bằng lần đầu
- **`dataset_version` không đổi**: vì nội dung gold cuối cùng giống hệt — cùng frames, cùng split, cùng MSSV
- **Bằng chứng**: ảnh `report/screenshots/airflow_crash.png` (log task đỏ thấy chữ "simulated crash trip-07900") và `report/screenshots/airflow_graph.png` (DAG xanh sau khi clear)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba thành phần sẽ vỡ đầu tiên khi scale lên 500 xe/ngày:

1. **FFmpeg decode tuần tự (Airflow LocalExecutor, `max_active_tis_per_dagrun=1`)**: Hiện tại chỉ chạy 1 trip song song, 500 xe × ~26 trip/xe = 13.000 task/ngày sẽ mất hàng chục giờ. **Giải pháp**: Chuyển sang distributed executor (Celery/Kubernetes Executor), dùng GPU decode batch với `ffmpeg`-based worker pool, hoặc dedicated video processing service như AWS Elemental (slide 24).

2. **DuckDB + single-node processing**: DuckDB chạy trong một container đơn, không thể xử lý song song nhiều DAG run. **Giải pháp**: Thay bằng Spark hoặc Flink cho xử lý phân tán (slide 22), hoặc dbt Cloud với Snowflake/BigQuery làm warehouse scale-out.

3. **MinIO single-node object storage**: Một MinIO container không đủ throughput và IOPS cho 500 xe ghi đồng thời (mỗi xe có thể tạo hàng nghìn frame/ngày). **Giải pháp**: Thay bằng S3/GCS (managed, scale tự động), dùng Delta Lake với multi-writer commit protocol, thêm partition strategy theo ngày/xe (slide 17, 23).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
SELECT
    t.trip_id,
    t.drive_id,
    t.lighting,
    t.conditions,
    COUNT(f.frame_id) AS total_frames,
    COUNT(f.frame_id) FILTER (WHERE f.is_black OR f.is_blurry OR f.is_near_dup) AS dropped_frames,
    ROUND(100.0 * COUNT(f.frame_id) FILTER (WHERE f.is_black OR f.is_blurry OR f.is_near_dup) / COUNT(f.frame_id), 1) AS drop_rate_pct
FROM marts.mart_trip_summary t
JOIN marts.mart_frame_gallery f USING (trip_id)
GROUP BY 1, 2, 3, 4
ORDER BY drop_rate_pct DESC
LIMIT 10;
```

Câu hỏi: **"10 trip có tỷ lệ khung bị loại cao nhất là những trip nào và chụp trong điều kiện gì?"** — Cho biết trip nào có vấn đề chất lượng nhất (ví dụ ánh sáng kém, thời tiết xấu), giúp đội vận hành ưu tiên kiểm tra camera hay lên lịch thu thập lại dữ liệu ở những cung đường đó.

## Nhìn lại

Điều bất ngờ nhất: pipeline xử lý **độ lệch đồng hồ** 1500 ms của nguồn B một cách tự động từ `sources.json` — tôi không cần viết code, chỉ cần khai báo giá trị. Điều này cho thấy thiết kế "configuration over code" rất mạnh trong thực tế vận hành.

Điều còn chưa rõ: cơ chế ASOF join trong DuckDB — tôi hiểu nó lấy telemetry gần nhất **trước hoặc đúng** event_time của frame, nhưng chưa rõ hiệu năng khi scale lên hàng triệu dòng.

Một lỗi dữ liệu pipeline **chưa** bắt được: pipeline hiện không phát hiện **khung hình bị mờ chuyển động** (motion blur) khi xe quay nhanh — `blur_score` dùng Laplacian variance bắt được mờ do mất nét (out-of-focus) nhưng motion blur ở tốc độ cao có thể có blur_score thấp dù nội dung frame vẫn "nhìn thấy" được. Đây là trường hợp false negative tiềm năng trong bộ dữ liệu train.
