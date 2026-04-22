/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// nodes.hpp

#include <fins/node.hpp>
#include <livox_driver2/msg/custom_msg.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <fstream>
#include <mutex>
#include <filesystem>

class LivoxRecorder : public fins::Node {
public:
  void define() override {
    set_name("LivoxRecorder");
    set_description("High-efficiency binary recorder for Livox CustomMsg and IMU.");
    set_category("Livox");

    register_input<livox_driver2::msg::CustomMsg>("lidar", &LivoxRecorder::on_livox);
    register_input<sensor_msgs::msg::Imu>("imu", &LivoxRecorder::on_imu);

    register_parameter<std::string>("file_path", &LivoxRecorder::on_file_changed, "data.lbag");
  }

  void initialize() override {
    open_file();
  }

  void reset() override {
    close_file();
  }

  void pause() override {
    close_file();
  }

  void run() override {
    // Nothing to do, we write on callbacks
  }

private:
  void on_file_changed(const std::string& v) {
    if (file_path_ == v) return;
    file_path_ = v;
    close_file();
    open_file();
  }

  void open_file() {
    std::lock_guard<std::mutex> lock(mtx_);
    ofs_.open(file_path_, std::ios::binary | std::ios::out);
    if (!ofs_.is_open()) {
      logger->error("Failed to open file for recording: {}", file_path_);
    }
  }

  void close_file() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (ofs_.is_open()) {
      ofs_.close();
    }
  }

  void on_livox(const livox_driver2::msg::CustomMsg& msg) {
    if (!ofs_.is_open()) return;
    
    std::lock_guard<std::mutex> lock(mtx_);
    uint8_t type = 0;
    uint64_t ts = static_cast<uint64_t>(msg.header.stamp.sec) * 1000000000ULL + msg.header.stamp.nanosec;
    
    ofs_.write(reinterpret_cast<const char*>(&type), sizeof(type));
    ofs_.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
    
    uint32_t point_num = msg.point_num;
    uint32_t fixed_size = sizeof(msg.timebase) + sizeof(msg.point_num) + sizeof(msg.lidar_id) + sizeof(msg.rsvd);
    uint32_t points_size = point_num * sizeof(livox_driver2::msg::CustomPoint);
    uint32_t total_data_size = fixed_size + points_size;
    
    ofs_.write(reinterpret_cast<const char*>(&total_data_size), sizeof(total_data_size));
    ofs_.write(reinterpret_cast<const char*>(&msg.timebase), sizeof(msg.timebase));
    ofs_.write(reinterpret_cast<const char*>(&msg.point_num), sizeof(msg.point_num));
    ofs_.write(reinterpret_cast<const char*>(&msg.lidar_id), sizeof(msg.lidar_id));
    ofs_.write(reinterpret_cast<const char*>(&msg.rsvd), sizeof(msg.rsvd));
    
    if (point_num > 0) {
      ofs_.write(reinterpret_cast<const char*>(msg.points.data()), points_size);
    }
  }

  void on_imu(const sensor_msgs::msg::Imu& msg) {
    if (!ofs_.is_open()) return;
    
    std::lock_guard<std::mutex> lock(mtx_);
    uint8_t type = 1;
    uint64_t ts = static_cast<uint64_t>(msg.header.stamp.sec) * 1000000000ULL + msg.header.stamp.nanosec;
    
    struct PackedImu {
        double orientation[4];
        double angular_velocity[3];
        double linear_acceleration[3];
    } packed;
    
    packed.orientation[0] = msg.orientation.x;
    packed.orientation[1] = msg.orientation.y;
    packed.orientation[2] = msg.orientation.z;
    packed.orientation[3] = msg.orientation.w;
    packed.angular_velocity[0] = msg.angular_velocity.x;
    packed.angular_velocity[1] = msg.angular_velocity.y;
    packed.angular_velocity[2] = msg.angular_velocity.z;
    packed.linear_acceleration[0] = msg.linear_acceleration.x;
    packed.linear_acceleration[1] = msg.linear_acceleration.y;
    packed.linear_acceleration[2] = msg.linear_acceleration.z;

    uint32_t size = sizeof(packed);
    ofs_.write(reinterpret_cast<const char*>(&type), sizeof(type));
    ofs_.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
    ofs_.write(reinterpret_cast<const char*>(&size), sizeof(size));
    ofs_.write(reinterpret_cast<const char*>(&packed), size);
  }

  std::string file_path_;
  std::ofstream ofs_;
  std::mutex mtx_;
};

class LivoxPlayer : public fins::Node {
public:
  void define() override {
    set_name("LivoxPlayer");
    set_description("High-efficiency binary player for Livox/IMU data.");
    set_category("Livox");

    register_output<livox_driver2::msg::CustomMsg>("lidar");
    register_output<sensor_msgs::msg::Imu>("imu");

    register_parameter<std::string>("file_path", &LivoxPlayer::on_file_changed, "data.lbag");
    register_parameter<double>("play_speed", &LivoxPlayer::on_speed_changed, 1.0);
  }

  void run() override {
    if (!ifs_.is_open()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      return;
    }

    uint8_t type;
    uint64_t ts;
    uint32_t size;

    if (!ifs_.read(reinterpret_cast<char*>(&type), sizeof(type))) {
      on_eof();
      return;
    }
    ifs_.read(reinterpret_cast<char*>(&ts), sizeof(ts));
    ifs_.read(reinterpret_cast<char*>(&size), sizeof(size));

    if (first_msg_time_ == 0) {
      first_msg_time_ = ts;
      start_real_time_ = std::chrono::steady_clock::now();
    }

    uint64_t elapsed_msg_ns = ts - first_msg_time_;
    auto now_real = std::chrono::steady_clock::now();
    uint64_t elapsed_real_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now_real - start_real_time_).count();
    
    int64_t wait_ns = (elapsed_msg_ns / play_speed_) - elapsed_real_ns;
    if (wait_ns > 0) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(wait_ns));
    }

    if (type == 0) { // Livox
      livox_driver2::msg::CustomMsg msg;
      msg.header.stamp.sec = ts / 1000000000ULL;
      msg.header.stamp.nanosec = ts % 1000000000ULL;
      msg.header.frame_id = lidar_frame_;

      ifs_.read(reinterpret_cast<char*>(&msg.timebase), sizeof(msg.timebase));
      ifs_.read(reinterpret_cast<char*>(&msg.point_num), sizeof(msg.point_num));
      ifs_.read(reinterpret_cast<char*>(&msg.lidar_id), sizeof(msg.lidar_id));
      ifs_.read(reinterpret_cast<char*>(&msg.rsvd), sizeof(msg.rsvd));
      
      msg.points.resize(msg.point_num);
      ifs_.read(reinterpret_cast<char*>(msg.points.data()), msg.point_num * sizeof(livox_driver2::msg::CustomPoint));
      
      send("lidar", msg);
    } else if (type == 1) { // IMU
      sensor_msgs::msg::Imu msg;
      msg.header.stamp.sec = ts / 1000000000ULL;
      msg.header.stamp.nanosec = ts % 1000000000ULL;
      msg.header.frame_id = lidar_frame_;

      struct PackedImu {
        double orientation[4];
        double angular_velocity[3];
        double linear_acceleration[3];
      } packed;

      ifs_.read(reinterpret_cast<char*>(&packed), sizeof(packed));
      msg.orientation.x = packed.orientation[0];
      msg.orientation.y = packed.orientation[1];
      msg.orientation.z = packed.orientation[2];
      msg.orientation.w = packed.orientation[3];
      msg.angular_velocity.x = packed.angular_velocity[0];
      msg.angular_velocity.y = packed.angular_velocity[1];
      msg.angular_velocity.z = packed.angular_velocity[2];
      msg.linear_acceleration.x = packed.linear_acceleration[0];
      msg.linear_acceleration.y = packed.linear_acceleration[1];
      msg.linear_acceleration.z = packed.linear_acceleration[2];

      send("imu", msg);
    }
  }

  void reset() override {
    if (ifs_.is_open()) {
      ifs_.close();
    }
    first_msg_time_ = 0;
  }

  void pause() override {
    if (ifs_.is_open()) {
      ifs_.close();
    }
  }

private:
  void on_file_changed(const std::string& v) {
    file_path_ = v;
    ifs_.close();
    ifs_.open(file_path_, std::ios::binary | std::ios::in);
    first_msg_time_ = 0;
  }
  
  void on_speed_changed(double v) {
    play_speed_ = v;
    first_msg_time_ = 0;
  }

  void on_eof() {
    if (loop_) {
      ifs_.clear();
      ifs_.seekg(0);
      first_msg_time_ = 0;
      logger->info("Looping playback.");
    } else {
      logger->info("Playback finished.");
    }
  }

  std::string file_path_;
  double play_speed_ = 1.0;
  bool loop_ = false;
  std::string lidar_frame_ = "livox_frame";
  
  std::ifstream ifs_;
  uint64_t first_msg_time_ = 0;
  std::chrono::steady_clock::time_point start_real_time_;
};

EXPORT_NODE(LivoxRecorder)
EXPORT_NODE(LivoxPlayer)
DEFINE_PLUGIN_ENTRY(fins::STATEFUL)