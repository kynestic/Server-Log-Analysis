Traces-apm:
    end_time, start_time
        start_time: "@timestamp trong file log"
        end_time: "event/ingested trong file log"
    trace_path_id
        trace_path: "Đường dẫn trace của transaction"
        container/id: "ID của container trong file log"
    Các cột tracstraction
        transaction/*: "Thông tin chi tiết về transaction"
    spans: None

Metrics-apm:
    traces: None
    logs: None
    Metrics: None
    Các cột request data
        host/*: "Thông tin liên quan đến host trong file log"
    Trạng thái dịch vụ
        service: "online nếu có, offline nếu không có"
    Thông tin agent/server
        agent/*: "Thông tin về agent trong file log"

Logs-apm:
    info
        message: "Trường message trong file log"
    warn: None
    ERROR, Stacktrace, mã lỗi, nguyên nhân
        exception/*: "Thông tin về exception, bao gồm mã lỗi và stacktrace"

Metrics-apm-usage-error-logs:
    cpu
        jvm.memory.used: "Trường này trong file log biểu thị bộ nhớ JVM đã sử dụng"
    ram
        process.runtime.jvm.cpu.utilization: "Trường này trong file log cho biết mức độ sử dụng CPU của JVM"
    latency
        event/ingested - @timestamp: "Chênh lệch thời gian giữa sự kiện và timestamp, chuyển đổi thành mili giây"
    error_rate: None
    response_time
        event/ingested: "Thời gian phản hồi từ sự kiện trong file log"
    requests_number
        http.server.duration/counts: "Số lượng yêu cầu (requests) từ file log"
