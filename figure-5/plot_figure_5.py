import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. ĐỊNH NGHĨA CÁC HỆ THỐNG VÀ THƯ MỤC LOG
# Script sẽ tự động tìm kiếm log trong các thư mục này.
# Bạn có thể thêm/xóa các hệ thống khác tại đây.
SYSTEM_CONFIGS = {
    "Veritas (Kafka)": {
        "log_dir": "veritas_kafka_logs",
        "file_prefix": "veritas-clients-", # Dùng để khớp tên file
    },
    "Veritas (TM)": {
        "log_dir": "veritas_tendermint_logs",
        "file_prefix": "veritas-tm-clients-",
    },
    "BigchainDB": {
        "log_dir": "bigchaindb_logs",
        "file_prefix": "bigchaindb-clients-",
    },
    "BigchainDB (PV)": {
        "log_dir": "bigchaindb_pv_logs",
        "file_prefix": "bigchaindb-pv-clients-",
    },
    "BlockchainDB": {
        "log_dir": "blockchaindb_logs",
        "file_prefix": "blockchaindb-clients-",
    },
}

def parse_log_file(filepath):
    """
    Đọc một file log duy nhất và trích xuất thông lượng (throughput) và độ trễ (latency).
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            
            # Sử dụng Regex để tìm các giá trị
            throughput_match = re.search(r'Throughput.*: (\d+) req/s', content)
            latency_match = re.search(r'Average latency: ([\d.]+) ms', content)
            
            if throughput_match and latency_match:
                throughput = int(throughput_match.group(1))
                latency = float(latency_match.group(1))
                return throughput, latency
                
    except Exception as e:
        print(f"  Lỗi khi đọc file {filepath}: {e}")
        
    return None, None

def collect_data():
    """
    Quét tất cả các thư mục log đã định nghĩa và thu thập dữ liệu.
    """
    all_data = []

    print("Bắt đầu quét log...")

    for system_name, config in SYSTEM_CONFIGS.items():
        log_dir = config["log_dir"]
        prefix = config["file_prefix"]
        
        if not os.path.isdir(log_dir):
            print(f"- Bỏ qua '{system_name}': Không tìm thấy thư mục '{log_dir}'")
            continue
            
        print(f"+ Đang xử lý '{system_name}' trong '{log_dir}'...")
        
        # Regex để lấy số client từ tên file
        # (Ví dụ: 'veritas-clients-256.txt' -> 256)
        file_regex = re.compile(f'^{re.escape(prefix)}(\d+)\.txt$')

        for filename in os.listdir(log_dir):
            match = file_regex.match(filename)
            if match:
                clients = int(match.group(1))
                filepath = os.path.join(log_dir, filename)
                
                throughput, latency = parse_log_file(filepath)
                
                if throughput is not None:
                    all_data.append({
                        "system": system_name,
                        "clients": clients,
                        "throughput": throughput,
                        "latency": latency
                    })

    print("...Quét log hoàn tất!")
    return all_data

def plot_charts(data):
    """
    Sử dụng dữ liệu đã thu thập để vẽ và lưu biểu đồ.
    """
    if not data:
        print("Không tìm thấy dữ liệu benchmark hợp lệ. Đã dừng vẽ biểu đồ.")
        return

    # Chuyển đổi sang Pandas DataFrame để dễ dàng vẽ
    df = pd.DataFrame(data)
    
    # Sắp xếp theo số client để biểu đồ đường vẽ đúng thứ tự
    df = df.sort_values(by="clients")

    print("\nDataFrame dữ liệu đã trích xuất:")
    print(df)

    # Thiết lập giao diện biểu đồ
    sns.set_theme(style="whitegrid")
    
    # Tạo 2 biểu đồ (1 hàng, 2 cột) giống như Figure 5 trong bài báo
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    
    # --- Biểu đồ 1: Thông lượng (Throughput) - Tương tự Figure 5(a) ---
    sns.lineplot(
        data=df, 
        x="clients", 
        y="throughput", 
        hue="system", 
        style="system",
        markers=True, 
        dashes=True, 
        ax=ax1,
        linewidth=2.5
    )
    ax1.set_title('Figure 5(a): Ảnh hưởng của Số lượng Client (Thông lượng)', fontsize=16)
    ax1.set_xlabel('Số lượng Client (Concurrency)', fontsize=12)
    ax1.set_ylabel('Thông lượng (TPS)', fontsize=12)
    ax1.set_yscale('log') # Sử dụng trục Y logarit giống bài báo
    ax1.legend(title='Hệ thống')
    ax1.get_xaxis().set_major_formatter(plt.ScalarFormatter()) # Hiển thị số chẵn
    ax1.get_yaxis().set_major_formatter(plt.ScalarFormatter()) # Hiển thị số chẵn

    # --- Biểu đồ 2: Độ trễ (Latency) - Tương tự Figure 5(b) ---
    sns.lineplot(
        data=df, 
        x="clients", 
        y="latency", 
        hue="system", 
        style="system",
        markers=True, 
        dashes=True, 
        ax=ax2,
        linewidth=2.5
    )
    ax2.set_title('Figure 5(b): Ảnh hưởng của Số lượng Client (Độ trễ)', fontsize=16)
    ax2.set_xlabel('Số lượng Client (Concurrency)', fontsize=12)
    ax2.set_ylabel('Độ trễ Trung bình (ms)', fontsize=12)
    # ax2.set_yscale('log') # Bật dòng này nếu bạn muốn độ trễ cũng ở trục log
    ax2.legend(title='Hệ thống')
    ax2.get_xaxis().set_major_formatter(plt.ScalarFormatter())

    # Tinh chỉnh layout và lưu file
    plt.tight_layout()
    output_filename = "figure_5_clients_benchmark.png"
    plt.savefig(output_filename)
    
    print(f"\n✅ Đã vẽ biểu đồ thành công và lưu tại: {output_filename}")

# --- Hàm chính để chạy ---
if __name__ == "__main__":
    benchmark_data = collect_data()
    plot_charts(benchmark_data)