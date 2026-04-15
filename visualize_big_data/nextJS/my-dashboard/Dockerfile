# 1. Dùng image Node.js bản nhẹ
FROM node:20.9.0-slim

# 2. Tạo thư mục làm việc trong container
WORKDIR /app

# 3. Copy package.json và cài đặt thư viện
COPY package*.json ./
RUN npm install

# 4. Copy toàn bộ code vào container
COPY . .

# 5. Build dự án Next.js
RUN npm run build

# 6. Mở cổng 3000
EXPOSE 3000

# 7. Chạy ứng dụng
CMD ["npm", "start"]