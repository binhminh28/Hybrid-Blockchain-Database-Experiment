import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# 1. ĐỊNH NGHĨA THƯ MỤC LOG CHÍNH
# Script sẽ quét bên trong thư mục này
BASE_LOG_DIR = "veritas_kafka_nodes_logs"

def parse_kafka_counters(filepath):
    """
    Đọc file 'kafka-counters.log' và trích xuất tổng số Read và Write.
    Cách tính toán dựa trên hướng dẫn trong README.md của tác giả.
    """
    
    # Read Count = Tổng của tất cả các giá trị CURRENT-OFFSET
    # Write Count = Giá trị lớn nhất của LOG-END-OFFSET
    
    total_read_count = 0
    max_write_count = 0
    
    try:
        with open(filepath, 'r') as f:
            for line in f:
                # Bỏ qua các dòng tiêu đề hoặc không liên quan
                if "CURRENT-OFFSET" in line or "GROUP" in line or not line.strip():
                    continue
                
                # Các dòng dữ liệu thường chứa 'rdkafka' (từ log của bạn)
                if "rdkafka" in line:
                    parts = line.split()
                    if len(parts) >= 5:
                        try:
                            # Vị trí cột dựa trên log mẫu
                            current_offset = int(parts[3])
                            log_end_offset = int(parts[4])
                            
                            total_read_count += current_offset
                            if log_end_offset > max_write_count:
                                max_write_count = log_end_offset
                        except (ValueError, IndexError):
                            continue # Bỏ qua nếu dòng bị định dạng sai
                            
    except Exception as e:
        print(f"    Lỗi khi đọc file {filepath}: {e}")
        return None, None

    # Trả về 0 nếu không tìm thấy gì (để tránh lỗi)
    if total_read_count == 0 and max_write_count == 0:
        return None, None
        
    return total_read_count, max_write_count

def collect_data():
    """
    Quét thư mục log chính và các thư mục con để thu thập dữ liệu Kafka.
    """
    all_data = []

    print("Bắt đầu quét log (Thí nghiệm Kafka Ops)...")

    if not os.path.isdir(BASE_LOG_DIR):
        print(f"- Lỗi: Không tìm thấy thư mục log chính '{BASE_LOG_DIR}'")
        return []
            
    # Regex để lấy số node từ tên thư mục con
    # (Ví dụ: 'veritas-nodes-14-logs' -> 14)
    dir_regex = re.compile(r'^veritas-nodes-(\d+)-logs$')

    for sub_dir_name in os.listdir(BASE_LOG_DIR):
        sub_dir_path = os.path.join(BASE_LOG_DIR, sub_dir_name)
        
        if os.path.isdir(sub_dir_path):
            match = dir_regex.match(sub_dir_name)
            
            if match:
                nodes = int(match.group(1))
                counters_file = os.path.join(sub_dir_path, "kafka-counters.log")
                
                if not os.path.exists(counters_file):
                    print(f"  ! Cảnh báo: Không tìm thấy 'kafka-counters.log' trong '{sub_dir_name}'")
                    continue
                    
                print(f"+ Đang xử lý log cho {nodes} node...")
                read_count, write_count = parse_kafka_counters(counters_file)
                
                if read_count is not None:
                    # Thêm 2 dòng riêng biệt (1 cho Read, 1 cho Write)
                    all_data.append({
                        "Nodes": nodes,
                        "Operation Type": "Read Count",
                        "Operations": read_count
                    })
                    all_data.append({
                        "Nodes": nodes,
                        "Operation Type": "Write Count",
                        "Operations": write_count
                    })

    print("...Quét log hoàn tất!")
    return all_data

def plot_charts(data):
    """
    Sử dụng dữ liệu đã thu thập để vẽ biểu đồ Figure 8.
    """
    if not data:
        print("Không tìm thấy dữ liệu benchmark hợp lệ. Đã dừng vẽ biểu đồ.")
        return

    # Chuyển đổi sang Pandas DataFrame để dễ dàng vẽ
    df = pd.DataFrame(data)
    
    # Sắp xếp theo số node để biểu đồ đường vẽ đúng thứ tự
    df = df.sort_values(by="Nodes")
    
    # Lấy danh sách các mốc node (ví dụ: 4, 8, 14) để đặt trục X
    node_ticks = sorted(df['Nodes'].unique())

    print("\nDataFrame dữ liệu đã trích xuất:")
    print(df)

    # Thiết lập giao diện biểu đồ
    sns.set_theme(style="whitegrid")
    
    # Tạo 1 biểu đồ duy nhất
    plt.figure(figsize=(10, 7))
    
    ax = sns.lineplot(
        data=df, 
        x="Nodes", 
        y="Operations", 
        hue="Operation Type", # Read Count vs Write Count
        style="Operation Type",
        markers=True, 
        dashes=True, 
        linewidth=2.5
    )
    
    ax.set_title('Figure 8: Ảnh hưởng của Số Node (Thao tác Kafka)', fontsize=16)
    ax.set_xlabel('Số lượng Node (Server)', fontsize=12)
    ax.set_ylabel('Số lượng Thao tác Kafka', fontsize=12)
    ax.set_yscale('log') # Sử dụng trục Y logarit giống bài báo
    ax.legend(title='Loại Thao tác')
    
    # *** THAY ĐỔI THEO YÊU CẦU ***
    # 1. Đặt mốc trục X (trục hoành) cho chính xác (ví dụ: 4, 8, 14)
    ax.set_xticks(node_ticks)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    
    # 2. Đặt mốc trục Y (trục tung) để hiển thị số (ví dụ: 1000000) thay vì (10^6)
    ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))

    # Tinh chỉnh layout và lưu file
    plt.tight_layout()
    output_filename = "figure_8_kafka_ops_benchmark.png"
    plt.savefig(output_filename)
    
    print(f"\n✅ Đã vẽ biểu đồ thành công và lưu tại: {output_filename}")

# --- Hàm chính để chạy ---
if __name__ == "__main__":
    benchmark_data = collect_data()
    plot_charts(benchmark_data)