# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Trần Thị Lan` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602621` |
| GitHub | `nan-bi` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07900` |

## Kết quả cuối

- `dataset_version` của bạn: `1b3e46f81d3f`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `train: 686 (18 trips), val: 167 (4 trips), test: 166 (4 trips) — tổng: 1019 khung hình (26 trips)`
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
- **Độ lệch đồng hồ**: `telemetry_clock_offset_ms = 1500 ms` (khai báo trong `sources.json`) được đọc từ `bronze.video_files` và trừ đi, khiến thời điểm bắt đầu của B khớp với video (`2025-10-08 14:30:00 UTC`). Bằng chứng là mẫu telemetry đầu tiên của B xuất hiện lúc `2025-10-08 14:30:00.009000+00:00`, chỉ lệch video 9 ms (trong ngưỡng cho phép 500 ms).
- **Bàn đạp**: B chia 100 để đưa về tỷ lệ 0–1.
- **Tên cột**: tất cả đổi về schema thống nhất (`lat`, `lon`, `acc_x`, `acc_y`, `gas`, `brake`, `steer`).

Bảng quarantine (số liệu thực tế từ `report/evidence/run_summary.json`):

| drive_id | reject_reason | số dòng |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

Tổng số dòng nhận: drive_a nhận 3021 dòng (2968 sạch + 53 quarantine); drive_b nhận 3234 dòng (3181 sạch + 53 quarantine). Tổng clean + quarantined đúng bằng số dòng thô từ CSV.

Dòng lỗi vào `silver.silver_telemetry_rejects` thay vì bị xoá vì: **dữ liệu xấu vẫn là bằng chứng**. Nếu xoá im lặng, không ai biết bao nhiêu dòng bị bỏ và vì lý do gì. Quarantine cho phép kiểm toán (audit), tái hiện vấn đề, và sau này có thể khôi phục nếu luật cách ly thay đổi. Đây là nguyên tắc "never silently drop data" của kiến trúc medallion.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Trong `dbt/models/schema.yml`, test `max_gap_ms` có cấu hình `severity: warn` còn `in_range` không có cấu hình đặc biệt nên mặc định là `error`. Lý do:
- `max_gap_ms` kiểm tra khoảng cách thời gian tối đa giữa hai dòng telemetry liên tiếp. **Khoảng cách lớn là điều bình thường** khi cảm biến mất tín hiệu ngắn (GPS loss, nhiễu hầm) — pipeline vẫn có thể tiếp tục hoạt động bình thường, chỉ cần đưa cảnh báo (WARN) để người vận hành lưu ý.
- `in_range` kiểm tra tốc độ trong khoảng hợp lệ (0–70 m/s). Tốc độ âm hoặc vượt 70 m/s là **dữ liệu vô lý về mặt vật lý**, không thể xảy ra đối với xe thật; nếu để lọt vào gold sẽ làm méo mó các phân phối dữ liệu và làm hỏng mô hình học máy.

Kết quả thí nghiệm `break-gate` (từ `report/evidence/break_gate.json`):
Khi nới luật làm sạch từ 70 m/s lên 1000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` đã **bắt được 6 dòng** tốc độ vô lý lọt vào silver (3 dòng ở drive_a và 3 dòng ở drive_b). Kết quả kiểm tra dbt test: `PASS=5 WARN=1 ERROR=1 SKIP=0`. Test báo ERROR và chặn đứng pipeline ngay tại cổng chất lượng của silver. Sau khi khôi phục lại luật gốc (`max_speed_mps = 70`), kết quả trả về `PASS=6 WARN=1 ERROR=0 SKIP=0`. Điều này chứng minh cổng chất lượng hoạt động như một chốt chặn bảo vệ: dữ liệu không đạt chuẩn không bao giờ được phép đi tiếp sang lớp gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào `report/evidence/idempotency.json` thu được từ thí nghiệm:

- **Số dòng trước khi chạy lại**: `1246` dòng toàn bảng `silver.frames`
- **Sau 2 lần chạy lại cùng trip (trip-00003)**: `1246` dòng — **hoàn toàn không đổi**
- **Dấu vân tay (fingerprint)**: `6c7480dfde7e` → `6c7480dfde7e` — **giống hệt nhau**
- **Cách sai (append × 2)**: trip có 60 khung hình ban đầu bị nhân đôi thành `120` dòng (tăng 2x dữ liệu rác)

Delta Lake `overwrite` theo predicate (`WHERE trip_id = 'trip-xxx'`) an toàn hơn "xoá rồi ghi" vì: (1) Thao tác là **atomic (nguyên tử)** qua transaction log của Delta Lake — hoặc commit toàn bộ thành công hoặc rollback hoàn toàn, không có trạng thái nửa chừng. (2) Nếu tiến trình sập giữa chừng, transaction chưa được commit nên phiên bản trước đó vẫn nguyên vẹn; người dùng truy vấn vẫn thấy dữ liệu cũ hợp lệ thay vì thấy bảng trống. (3) **Chỉ partition đó** bị ghi đè, 25 trip còn lại trong lake hoàn toàn không bị ảnh hưởng. Ngược lại, cách "xoá rồi ghi lại" tạo ra cửa sổ thời gian bảng rỗng (downtime), và nếu việc ghi lại gặp lỗi, dữ liệu sẽ mất vĩnh viễn mà không thể tự phục hồi.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

**Khung đen** (`is_black`): độ sáng trung bình (luma) dưới ngưỡng `dark_luma = 12.0` — camera bị che, hỏng cáp, hoặc đầu ghi lỗi kỹ thuật. Khung này hoàn toàn mất thông tin hình ảnh, không có giá trị học máy → pipeline gắn cờ và **loại bỏ khỏi gold** (drive_a có 6 khung, drive_b có 4 khung).

**Khung tối** (`is_dark`): luma trong khoảng [12, 40) — xe đi qua đường hầm, bóng cây, hoặc điều kiện chập tối. Đây là **tình huống vận hành thực tế** mà mô hình tự lái bắt buộc phải học để nhận diện đường trong điều kiện thiếu sáng → pipeline **gắn cờ chất lượng nhưng giữ lại** (`dark_kept`: drive_a có 4 khung, drive_b có tới 61 khung). Tôi hoàn toàn đồng ý với thiết kế này vì nó phân biệt rành mạch giữa lỗi phần cứng (đen) và điều kiện môi trường tự nhiên (tối).

Số khung bị coi là trùng theo ngưỡng dHash (số liệu từ `report/evidence/threshold.json`):

| dHash khác tối đa | số khung trùng, drive A (tổng 602) | số khung trùng, drive B (tổng 644) |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Pipeline chọn ngưỡng **2 bit** (`dup_hamming = 2`). Đây là lựa chọn tối ưu vì: ngưỡng 0 bit chỉ bắt các khung hoàn toàn giống nhau từng pixel sau hash (quá chặt, bỏ sót các khung xe đứng yên nhưng nhiễu sensor nhẹ); trong khi ngưỡng 6 bit quá lỏng, loại tới hơn 50% khung hình và làm mất cả những khung xe đang vào cua hay chuyển động chậm. Về tốc độ xe: khi xe chạy chậm (< 10 km/h), khoảng cách di chuyển giữa 2 khung hình (0.5s) chỉ khoảng 1.4 m nên độ tương đồng hình ảnh rất cao → khung trùng nhiều (drive_a xe đi chậm và bị đứng hình camera 6s nên có tới 149 khung trùng ở t2). Khi xe chạy nhanh (60-80 km/h), mỗi 0.5s xe đi được 8-11 m, cảnh vật thay đổi liên tục → tỷ lệ khung trùng giảm rõ rệt (drive_b chỉ có 53 khung trùng ở t2).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào `report/evidence/leakage.json` từ thí nghiệm:

- **Chia theo khung hình ngẫu nhiên** (random 70/15/15): có **26/26 trip** bị chia cắt ra nhiều hơn một split; và toàn bộ **26/26 trip** đều có khung hình xuất hiện ở cả tập train lẫn test (100% số trip bị rò rỉ).
- **Chia theo trip** (gold pipeline): **0 trip** bị chia cắt — mỗi trip trọn vẹn chỉ nằm trong đúng một split duy nhất.

Khi chia theo khung hình ngẫu nhiên, các khung hình kế cận nhau trong cùng một chuyến đi chỉ cách nhau 0.5 giây và gần như giống hệt nhau về quang cảnh, góc nhìn, phương tiện xung quanh và ánh sáng. Việc đưa khung này vào train và khung liền kề vào test thực chất là làm "lộ đề thi": mô hình deep learning có khuynh hướng ghi nhớ (memorize) các chi tiết cảnh nền cụ thể hơn là học các quy luật lái xe tổng quát. Khi đánh giá trên tập test bị rò rỉ, mô hình đạt độ chính xác rất cao giả tạo (optimistic metric), nhưng khi đem triển khai trên xe thật chạy ở một cung đường mới lạ, hệ thống sẽ thất bại nghiêm trọng. Chia theo trip (cảnh) đảm bảo tập test kiểm tra đúng năng lực tổng quát hóa (generalization) của mô hình.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là **chuỗi băm nội dung (content hash)** (12 ký tự hex) vì các nguyên do thiết kế lakehouse cốt lõi:
1. **Tính tất định (Deterministic & Idempotent)**: Cùng một tập dữ liệu đầu vào và luật xử lý sẽ luôn cho ra cùng một chuỗi băm, bất kể chạy lại bao nhiêu lần, ở múi giờ nào hay trên máy nào.
2. **Định danh theo nội dung (Content-addressable)**: Phiên bản chỉ thay đổi khi và chỉ khi dữ liệu hoặc nhãn thực sự thay đổi. Nếu dùng ngày giờ hoặc số đếm tăng dần, mỗi lần trigger pipeline sẽ sinh ra một phiên bản mới dù dữ liệu y hệt, gây lãng phí lưu trữ và nhầm lẫn cho việc huấn luyện mô hình.
3. **Khả năng kiểm chứng độc lập**: Giảng viên có thể chạy lại pipeline với cùng MSSV của tôi và kiểm tra xem chuỗi hash có khớp chính xác `1b3e46f81d3f` hay không.

**Hai lần chạy liên tiếp** với cùng MSSV và cùng bộ dữ liệu cho ra **cùng `dataset_version` = `1b3e46f81d3f`** — xác nhận tính deterministic tuyệt đối.

`dataset_version` của tôi khác bạn bè vì: trong `dbt/models/gold/gold_dataset_frames.sql`, thứ tự trip được xáo trộn bằng hàm `md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')`. MSSV `2A202602621` đóng vai trò là "salt" làm thay đổi thứ tự sắp xếp ngẫu nhiên của các trip trong từng drive → phân bổ trip vào train/val/test khác nhau → tập frame_id và manifest khác nhau → chuỗi hash nội dung cuối cùng là duy nhất cho từng sinh viên.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Trích xuất trực tiếp từ `lake/gold/datasets/1b3e46f81d3f/manifest.json`:

Dataset phiên bản `1b3e46f81d3f` được tạo thành từ:
- **Video nguồn**: 
  + `drive_a.mp4`: sha256 = `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  + `drive_b.mp4`: sha256 = `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- **Các phiên bản bảng upstream (Delta commit versions)**:
  + `bronze.raw_telemetry_a`: v0, `bronze.raw_telemetry_b`: v0, `bronze.trips_raw`: v0, `bronze.video_files`: v1
  + `silver.frames`: v25, `silver.telemetry`: v0, `silver.frames_flagged`: v0, `silver.frame_telemetry`: v0
- **Tham số quy hoạch**: `split_train = 0.70`, `split_val = 0.15`, `dup_hamming = 2`, `sync_tol_ms = 150`, `LAB_STUDENT_ID = 2A202602621`
- **Thời điểm tạo**: `2026-10-12T13:41:34.196448+00:00`
- **Mã băm manifest**: sha256 = `b3246f5dafc79379d1af4a5b4b4d332609d34527093315881a620407531a1eff`

Nếu một người hỏi sau 3 tháng, những thông tin cần thiết để tái tạo chính xác 100% dataset này gồm: (1) Hai file video gốc có đúng mã sha256 nêu trên; (2) Đúng MSSV `2A202602621` trong biến môi trường; (3) Đúng commit mã nguồn pipeline và dbt models; (4) Khả năng time-travel trong Delta Lake quay về đúng các commit version của từng bảng bronze và silver được ghi nhận trong mục `lineage`.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip được gán cho MSSV của tôi: `2A202602621 % 26 → trip-07900` (thuộc drive_a).

Khi đặt biến môi trường `LAB_FAIL_ONCE=trip-07900` và kích hoạt DAG `day09_video_pipeline`:
- **Đúng 1 task** `silver_frames[trip-07900]` bị thất bại và chuyển **màu đỏ** (trong log ghi rõ: `simulated crash while extracting trip-07900`).
- Các task độc lập khác (các trip khác) vẫn chạy bình thường và chuyển xanh.
- Toàn bộ **7 task downstream** phụ thuộc phía sau bị dừng lại và chuyển trạng thái **`upstream_failed`** gồm: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`.
- Khi tôi nhấn **Clear** đối với duy nhất task đỏ `silver_frames[trip-07900]`, Airflow chỉ lên lịch chạy lại riêng task này và các task phụ thuộc downstream, không phải chạy lại 25 task trích xuất khác.
- **Dữ liệu không bị nhân đôi**: Nhờ cơ chế Delta overwrite theo partition `trip_id = 'trip-07900'`, số khung hình ghi vào lake vẫn giữ nguyên là 1246 khung hình.
- **`dataset_version` không đổi**: vẫn là `1b3e46f81d3f` vì dữ liệu cuối cùng và hash nội dung không thay đổi.
- **Bằng chứng**: Ảnh `report/screenshots/airflow_crash_log.png` ghi lại log lỗi của task trip-07900 và `report/screenshots/airflow_graph.png` thể hiện toàn bộ DAG xanh hoàn chỉnh.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi mở rộng quy mô từ 2 chuyến xe lên 500 xe/ngày (khoảng 13.000 chuyến, hàng triệu khung hình và hàng chục GB telemetry mỗi ngày):

1. **Bộ giải mã FFmpeg cục bộ (LocalExecutor trên 1 máy)** (Slide 24): Hiện tại FFmpeg decode trên CPU tuần tự từng trip. Với 13.000 trip/ngày, thời gian CPU cần thiết sẽ lên đến hàng trăm giờ máy, gây nghẽn hoàn toàn. **Giải pháp**: Tách video decoding thành một microservice phi tập trung (serverless worker pool trên Kubernetes / AWS Batch), sử dụng card GPU chuyên dụng (NVDEC) để tăng tốc decode phần cứng, kết hợp distributed queue như Kafka/Celery để phân tải.

2. **DuckDB chạy trên một node đơn lẻ** (Slide 22): DuckDB là embedded in-process database rất nhanh nhưng bị giới hạn bởi RAM và CPU của máy chủ chứa container. Khi xử lý hàng trăm triệu dòng telemetry và hàng triệu bounding box/frame, bộ nhớ sẽ bị tràn (OOM). **Giải pháp**: Chuyển đổi tầng ELT sang công nghệ tính toán phân tán như Apache Spark (Databricks) hoặc Apache Flink để xử lý streaming/batch song song trên cụm nhiều node.

3. **MinIO single-node lưu trữ đối tượng** (Slide 17, 23): Một MinIO container cục bộ sẽ nhanh chóng cạn kiệt dung lượng đĩa cứng và nghẽn băng thông I/O mạng khi hàng trăm xe đồng thời đẩy dữ liệu lên. **Giải pháp**: Chuyển sang dịch vụ Object Storage đám mây thực thụ (AWS S3, Google Cloud Storage) có khả năng scale vô hạn về throughput và dung lượng, áp dụng chính sách vòng đời lưu trữ (Lifecycle rules: hot/warm/cold tiering) để tối ưu chi phí lưu trữ lâu dài.

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
