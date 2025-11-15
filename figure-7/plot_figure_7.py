import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker  # Import thư viện định dạng

# 1. ĐỊNH NGHĨA CÁC HỆ THỐNG VÀ THƯ MỤC LOG
# Script sẽ tự động tìm kiếm log trong các thư mục này.
SYSTEM_CONFIGS = {
    "Veritas (Kafka)": {
        "log_dir": "veritas_kafka_logs",
        "file_prefix": "veritas-nodes-", # Khớp với 'veritas-nodes-4.txt'
    },
    "Veritas (TM)": {
        "log_dir": "veritas_tendermint_logs",
        "file_prefix": "veritas-tm-nodes-", # Khớp với 'veritas-tm-nodes-4.txt'
    },
    "BigchainDB": {
        "log_dir": "bigchaindb_logs",
        "file_prefix": "bigchaindb-nodes-", # Tên giả định
    },
    "BigchainDB (PV)": {
        "log_dir": "bigchaindb_pv_logs",
        "file_prefix": "bigchaindb-pv-nodes-", # Tên giả định
    },
    "BlockchainDB": {
        "log_dir": "blockchaindb_logs",
        "file_prefix": "blockchaindb-nodes-", # Tên giả định
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

    print("Bắt đầu quét log (Thí nghiệm Nodes)...")

    for system_name, config in SYSTEM_CONFIGS.items():
        log_dir = config["log_dir"]
        prefix = config["file_prefix"]
        
        if not os.path.isdir(log_dir):
            print(f"- Bỏ qua '{system_name}': Không tìm thấy thư mục '{log_dir}'")
            continue
            
        print(f"+ Đang xử lý '{system_name}' trong '{log_dir}'...")
        
        # Regex để lấy số node từ tên file
        # (Ví dụ: 'veritas-nodes-14.txt' -> 14)
        file_regex = re.compile(f'^{re.escape(prefix)}(\d+)\.txt$')

        for filename in os.listdir(log_dir):
            match = file_regex.match(filename)
            if match:
                nodes = int(match.group(1))
                filepath = os.path.join(log_dir, filename)
                
                throughput, latency = parse_log_file(filepath)
                
                if throughput is not None:
                    all_data.append({
                        "system": system_name,
                        "nodes": nodes,
                        "throughput": throughput,
                        "latency": latency
                    })

    print("...Quét log hoàn tất!")
    return all_data

def plot_charts(data):
    """
    Sử dụng dữ liệu đã thu thập để vẽ và lưu biểu đồ.
    *** ĐÃ CẬP NHẬT ***: Định dạng lại trục X và Y theo yêu cầu.
    """
    if not data:
        print("Không tìm thấy dữ liệu benchmark hợp lệ. Đã dừng vẽ biểu đồ.")
        return

    # Chuyển đổi sang Pandas DataFrame để dễ dàng vẽ
    df = pd.DataFrame(data)
    
    # Sắp xếp theo số node để biểu đồ đường vẽ đúng thứ tự
    df = df.sort_values(by="nodes")
    
    # Lấy danh sách các mốc node (ví dụ: 4, 8, 14) để đặt trục X
    node_ticks = sorted(df['nodes'].unique())

    print("\nDataFrame dữ liệu đã trích xuất:")
    print(df)

    # Thiết lập giao diện biểu đồ
    sns.set_theme(style="whitegrid")
    
    # Tạo 2 biểu đồ (1 hàng, 2 cột) giống như Figure 7 trong bài báo
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle('Thí nghiệm 2: Ảnh hưởng của Số lượng Node (Figure 7)', fontsize=16)
    
    # --- Biểu đồ 1: Thông lượng (Throughput) - Tương tự Figure 7(a) ---
    sns.lineplot(
        data=df, 
        x="nodes", 
        y="throughput", 
        hue="system", 
        style="system",
        markers=True, 
        dashes=True, 
        ax=ax1,
        linewidth=2.5
    )
    ax1.set_title('Figure 7(a): Thông lượng (TPS)', fontsize=14)
    ax1.set_xlabel('Số lượng Node (Server)', fontsize=12)
    ax1.set_ylabel('Thông lượng (TPS)', fontsize=12)
    ax1.set_yscale('log') # Vẫn sử dụng trục Y logarit giống bài báo
    ax1.legend(title='Hệ thống')
    
    # *** THAY ĐỔI THEO YÊU CẦU ***
    # 1. Đặt mốc trục X (trục hoành) cho chính xác (ví dụ: 4, 8, 14)
    ax1.set_xticks(node_ticks)
    ax1.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    
    # 2. Đặt mốc trục Y (trục tung) để hiển thị số (ví dụ: 10000) thay vì (10^4)
    ax1.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))


    # --- Biểu đồ 2: Độ trễ (Latency) - Tương tự Figure 7(b) ---
    sns.lineplot(
        data=df, 
        x="nodes", 
        y="latency", 
        hue="system", 
        style="system",
        markers=True, 
        dashes=True, 
        ax=ax2,
        linewidth=2.5
    )
    ax2.set_title('Figure 7(b): Độ trễ (Latency)', fontsize=14)
    ax2.set_xlabel('Số lượng Node (Server)', fontsize=12)
    ax2.set_ylabel('Độ trễ Trung bình (ms)', fontsize=12)
    ax2.legend(title='Hệ thống')

    # *** THAY ĐỔI THEO YÊU CẦU ***
    # 1. Đặt mốc trục X (trục hoành) cho chính xác
    ax2.set_xticks(node_ticks)
    ax2.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    
    # 2. Đảm bảo trục Y (latency) cũng hiển thị số chẵn
    ax2.get_yaxis().set_major_formatter(ticker.ScalarFormatter())


    # Tinh chỉnh layout và lưu file
    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Điều chỉnh layout cho tiêu đề
    output_filename = "figure_7_nodes_benchmark.png"
    plt.savefig(output_filename)
    
    print(f"\n✅ Đã vẽ biểu đồ thành công và lưu tại: {output_filename}")

# --- Hàm chính để chạy ---
if __name__ == "__main__":
    benchmark_data = collect_data()
    plot_charts(benchmark_data)