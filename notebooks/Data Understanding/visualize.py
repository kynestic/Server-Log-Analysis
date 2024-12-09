import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class Visualize:
    def __init__(self, log_path):
        self.log_df = pd.read_csv(log_path)
        self.log_df['datetime'] = pd.to_datetime(self.log_df['datetime'], errors='coerce') 
        self.log_df['hour'] = self.log_df['datetime'].dt.hour
    
    def plot_errors_over_time(self):
        """
        Vẽ biểu đồ đường thể hiện số lượng lỗi và request hợp lệ qua thời gian.
        """
        self.log_df['request_type'] = self.log_df['status'].apply(lambda x: 'Lỗi' if x >= 400 else 'Hợp lệ')
        hourly_counts = self.log_df.groupby(['hour', 'request_type']).size().unstack(fill_value=0)
        
        # In ra dữ liệu
        print("Số lượng lỗi và request hợp lệ qua từng giờ:")
        print(hourly_counts)

        plt.figure(figsize=(10, 6))
        sns.lineplot(data=hourly_counts, markers=True, dashes=False)
        plt.title("Số lượng lỗi và Request hợp lệ qua thời gian")
        plt.xlabel("Giờ")
        plt.ylabel("Số Lượng")
        plt.xticks(range(24))
        plt.legend(title="Loại Request", labels=["Hợp lệ", "Lỗi"])
        plt.grid(True)
        plt.show()

    def plot_request_distribution(self):
        """
        Vẽ biểu đồ tròn thể hiện phân bố các loại request (GET, POST, PUT, DELETE,...).
        """
        request_counts = self.log_df['method'].value_counts()
        
        # In ra dữ liệu
        print("Phân bố các loại request:")
        print(request_counts)

        plt.figure(figsize=(8, 8))
        request_counts.plot.pie(autopct='%1.1f%%', startangle=90, colors=sns.color_palette("pastel"))
        plt.title("Phân bố các loại Request")
        plt.ylabel("")
        plt.show()

    def plot_bytes_sent_per_hour(self):
        """
        Vẽ biểu đồ đường thể hiện tổng lượng dữ liệu gửi đi mỗi giờ.
        """
        hourly_bytes_sent = self.log_df.groupby('hour')['bytes_sent'].sum()
        
        # In ra dữ liệu
        print("Tổng lượng dữ liệu gửi đi mỗi giờ:")
        print(hourly_bytes_sent)

        plt.figure(figsize=(10, 6))
        sns.lineplot(x=hourly_bytes_sent.index, y=hourly_bytes_sent.values, marker='o', color='blue')
        plt.title("Tổng lượng dữ liệu gửi đi mỗi giờ")
        plt.xlabel("Giờ")
        plt.ylabel("Tổng số byte gửi đi")
        plt.xticks(range(24))
        plt.grid(True)
        plt.show()
    
    def plot_log_level(self):
        """
        Vẽ biểu đồ thanh thể hiện phân bố log level (DEBUG, INFO, WARNING, ERROR, FATAL).
        """
        def classify_log_level(status):
            if 200 <= status <= 299:
                return "INFO"
            elif 300 <= status <= 399:
                return "WARNING"
            elif 400 <= status <= 499:
                return "ERROR"
            elif 500 <= status <= 599:
                return "FATAL"
            else:
                return "DEBUG"
        
        self.log_df['log_level'] = self.log_df['status'].apply(classify_log_level)
        log_level_counts = self.log_df['log_level'].value_counts()

        # In ra dữ liệu
        print("Số lượng log từng loại:")
        print(log_level_counts)

        plt.figure(figsize=(10, 6))
        sns.barplot(x=log_level_counts.index, y=log_level_counts.values, palette="viridis")
        plt.title("Phân bố Log Level")
        plt.xlabel("Log Level")
        plt.ylabel("Số lượng")
        plt.grid(axis="y")
        plt.show()

    def plot_user_agent_distribution(self):
        """
        Vẽ biểu đồ thanh ngang thể hiện phân bố các loại user agent trong log.
        """
        user_agent_counts = self.log_df['user_agent'].value_counts().head(10)  # Lấy 10 user agent phổ biến nhất
        
        print("Số lượng log theo các loại user agent:")
        for agent, count in user_agent_counts.items():
            print(f"{agent}: {count}")
        
        plt.figure(figsize=(10, 6))
        sns.barplot(y=user_agent_counts.index, x=user_agent_counts.values, palette="viridis", orient='h')
        plt.title("Phân bố các loại User Agent")
        plt.xlabel("Số lượng")
        plt.ylabel("User Agent")
        plt.subplots_adjust(left=0.4)
        
        plt.grid(axis="x")
        plt.show()

# if __name__ == "__main__":
#     path = r'D:\CODING\Project\Server Log Analysis\data\interim\parsed_logs.csv'

#     visualizer = Visualize(path)

#     visualizer.plot_user_agent_distribution()