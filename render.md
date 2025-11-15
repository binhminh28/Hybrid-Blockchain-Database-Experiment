## 1. Phương pháp của chúng ta

Quy trình làm việc của chúng ta sẽ diễn ra theo 3 bước:

1.  **Bạn (Người chạy):** Bạn chạy các script benchmark (ví dụ: `./run_benchmark_veritas_kafka_clients.sh`).
2.  **Bạn (Người cung cấp):** Sau khi chạy xong, bạn sẽ `cd` vào thư mục log được tạo ra (ví dụ: `logs-clients-veritas-kafka-...`) và dùng lệnh `cat` để hiển thị nội dung các file log kết quả (ví dụ: `cat veritas-clients-*.txt`). Bạn sao chép (copy) và dán (paste) nội dung log đó cho tôi.
3.  **Tôi (Đối tác lập trình):**
    * **Phân tích (Parse):** Tôi sẽ đọc nội dung text bạn cung cấp.
    * **Trích xuất (Extract):** Tôi sẽ trích xuất các con số quan trọng (như `req/s` và `Average latency`).
    * **Viết code:** Tôi sẽ cung cấp cho bạn một đoạn mã **Python** (sử dụng thư viện `matplotlib` và `seaborn`) hoàn chỉnh. Bạn chỉ cần chạy file Python đó để tạo ra biểu đồ giống hệt như trong bài báo.

---

## 2. Kế hoạch chi tiết (Các Script và Biểu đồ tương ứng)

Đây là bản đồ chi tiết, liên kết từng script bạn chạy với biểu đồ tương ứng trong bài báo của tác giả:

### Thí nghiệm 1: Ảnh hưởng của Số lượng Client (Figure 5)
* **Mục tiêu:** Tái tạo Figure 5 (Biểu đồ đường về Thông lượng và Độ trễ khi client tăng).
* **Script bạn chạy:**
    * `./run_benchmark_veritas_kafka_clients.sh`
    * `./run_benchmark_veritas_tendermint_clients.sh`
* **Log bạn cung cấp:** Nội dung của các file `veritas-clients-*.txt` và `veritas-tm-clients-*.txt`.
* **Tôi sẽ làm:** Trích xuất TPS và Latency cho mỗi mốc (4, 8, 16... 256 client) và cung cấp mã Python để vẽ biểu đồ đường.

### Thí nghiệm 2: Ảnh hưởng của Số lượng Node (Figure 7)
* **Mục tiêu:** Tái tạo Figure 7 (Thông lượng và Độ trễ khi số node tăng).
* **Script bạn chạy:**
    * `./run_benchmark_veritas_kafka_nodes.sh`
    * `./run_benchmark_veritas_tendermint_nodes.sh`
* **Log bạn cung cấp:** Nội dung của các file `veritas-nodes-*.txt` và `veritas-tm-nodes-*.txt` (cho các node bạn đã cấu hình trong `env.sh`).
* **Tôi sẽ làm:** Trích xuất TPS và Latency cho mỗi mốc (4, 8, 14 node) và cung cấp mã Python.

### Thí nghiệm 3: Thao tác Kafka (Figure 8)
* **Mục tiêu:** Tái tạo Figure 8 (Số lượng đọc/ghi Kafka).
* **Script bạn chạy:** `run_benchmark_veritas_kafka_nodes.sh` (script này tự động thu thập log Kafka).
* **Log bạn cung cấp:** Bạn sẽ cần `cat` các file `kafka-counters.log` từ bên trong mỗi thư mục con (ví dụ: `veritas-nodes-4-logs/kafka-counters.log`, `veritas-nodes-8-logs/kafka-counters.log`, v.v.).
* **Tôi sẽ làm:** Trí Fch xuất tổng số `CURRENT-OFFSET` (Reads) và `LOG-END-OFFSET` (Writes) và cung cấp mã Python để vẽ biểu đồ.

### Thí nghiệm 4 & 5: Phân phối & Workload (Figure 9, 10, 11)
* **Mục tiêu:** Tái tạo Figure 9, 10, 11 (Biểu đồ cột so sánh các workload).
* **Script bạn chạy:**
    * `..._distribution.sh` (cho Kafka & TM)
    * `..._workload.sh` (cho Kafka & TM)
    * `..._database.sh` (cho Kafka)
* **Log bạn cung cấp:** Nội dung của các file `.txt` tương ứng (ví dụ: `veritas-uniform.txt`, `veritas-kafka-workloada.txt`, v.v.).
* **Tôi sẽ làm:** Trích xuất các giá trị TPS và cung cấp mã Python để vẽ biểu đồ cột.

### Thí nghiệm 6 & 7: Kích thước Giao dịch & Thời gian Xử lý (Figure 13, 14)
* **Mục tiêu:** Tái tạo Figure 13 & 14 (Ảnh hưởng của kích thước và độ trễ).
* **Script bạn chạy:**
    * `..._recordsize.sh` (cho Kafka & TM)
    * `..._proctime.sh` (cho Kafka & TM)
* **Log bạn cung cấp:** Nội dung của các file `.txt` tương ứng (ví dụ: `veritas-kafka-txsize-512B.txt`, `veritas-kafka-txdelay-10.txt`, v.v.).
* **Tôi sẽ làm:** Trích xuất TPS cho mỗi mốc và cung cấp mã Python để vẽ biểu đồ đường.

### Thí nghiệm 8: Mạng (Figure 15, 16)
* **Mục tiêu:** Tái tạo Figure 15 & 16 (Ảnh hưởng của RTT và Băng thông).
* **Script bạn chạy:**
    * `./run_benchmark_veritas_kafka_networking.sh`
    * `./run_benchmark_veritas_tendermint_networking.sh`
* **Log bạn cung cấp:** Đây là phần phức tạp nhất. Bạn cần `cd` vào **từng thư mục con** (ví dụ: `logs-10000-5ms`, `logs-1000-10ms`, v.v.) và `cat` file kết quả benchmark (ví dụ: `veritas-kafka-net.txt`) bên trong đó.
* **Tôi sẽ làm:** Thu thập dữ liệu TPS/Latency cho từng cặp (Bandwidth, RTT) và cung cấp mã Python để vẽ biểu đồ đường.

Hãy bắt đầu với thí nghiệm đầu tiên (`..._clients.sh`) bất cứ khi nào bạn sẵn sàng!