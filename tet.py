import asyncio
import json
import os
import random
import re
from typing import List, Optional
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup, NavigableString
from supabase import create_client

BASE_URL     = "https://trangvangvietnam.com"
SUPABASE_URL = ""
SUPABASE_KEY = ""
TABLE        = "companies"
DETAIL_WORKERS = 10
BATCH_SIZE     = 100
PROGRESS_FILE  = "progress.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
]

CATEGORIES = [
    ('484645', 'logistics-dich-vu-logistics', 'Công ty Logistics'),
    ('246160', 'van-tai-cong-ty-van-tai-va-dai-ly-van-tai', 'Công ty vận tải'),
    ('419935', 'chuyen-phat-nhanh-cong-ty-chuyen-phat-nhanh', 'Chuyển phát nhanh'),
    ('186010', 'bao-bi-nhua', 'Bao bì nhựa'),
    ('484507', 'bao-bi-giay', 'Bao bì giấy'),
    ('191570', 'in-bao-bi-cong-ty-thiet-ke-va-in-an-bao-bi', 'In ấn bao bì'),
    ('31960', 'oc-vit-bu-loong', 'Ốc vít, bù lông'),
    ('26360', 'vong-bi-bac-dan', 'Vòng bi, bạc đạn'),
    ('95260', 'moi-truong-cong-ty-moi-truong', 'Công ty môi trường'),
    ('256610', 'xu-ly-nuoc-xu-ly-nuoc-thai-he-thong-xu-ly-nuoc-nuoc-thai', 'Xử lý nước thải'),
    ('112370', 'may-mac-cac-cong-ty-may-mac', 'Công ty may mặc'),
    ('268180', 'may-dong-phuc-cong-ty-may-dong-phuc', 'May đồng phục'),
    ('112350', 'may-mac-nguyen-phu-lieu-may-mac', 'Phụ liệu may'),
    ('152060', 'co-khi--gia-cong-va-che-tao', 'Gia công cơ khí'),
    ('488209', 'co-khi-chinh-xac-gia-cong-chi-tiet-linh-kien-phu-tung-theo-yeu-cau', 'Cơ khí chính xác'),
    ('159730', 'khuon-mau', 'Khuôn mẫu'),
    ('107560', 'van-chuyen-hang-hoa-giao-nhan-van-chuyen-hang-hoa', 'Vận Chuyển Hàng Hóa, Giao Nhận Vận Chuyển Hàng Hóa(2531)'),
    ('75010', 'hai-quan-dich-vu-hai-quan-khai-thue-hai-quan', 'Hải Quan - Dịch Vụ Hải Quan, Khai Thuê Hải Quan(1091)'),
    ('226360', 'boc-xep-hang-hoa-bang-xe-nang-xe-cau', 'Bốc Xếp Hàng Hóa - Bằng Xe Nâng, Xe Cẩu(224)'),
    ('254810', 'kho-van-dich-vu-kho-van', 'Kho Vận - Dịch Vụ Kho Vận(169)'),
    ('3092374', 'dich-vu-mua-ho-hang-quoc-te', 'Dịch Vụ Mua Hộ Hàng Quốc Tế(17)'),
    ('485215', 'van-tai-duong-bo', 'Vận Tải Đường Bộ(1277)'),
    ('213810', 'van-tai-bien', 'Vận Tải Biển(1180)'),
    ('12560', 'van-tai-duong-hang-khong', 'Vận Tải Đường Hàng Không(554)'),
    ('68660', 'van-tai-container', 'Vận Tải Container(501)'),
    ('485218', 'van-tai-duong-sat', 'Vận Tải Đường Sắt(147)'),
    ('491248', 'van-chuyen-may-cong-trinh-xe-co-gioi', 'Vận Chuyển Máy Công Trình, Xe Cơ Giới(37)'),
    ('487081', 'van-tai-noi-dia', 'Vận Tải Nội Địa(503)'),
    ('487084', 'van-tai-quoc-te', 'Vận Tải Quốc Tế(363)'),
    ('487078', 'van-tai-da-phuong-thuc', 'Vận Tải Đa Phương Thức(196)'),
    ('487075', 'van-chuyen-hang-sieu-truong-sieu-trong-qua-kho', 'Vận Chuyển Hàng Siêu Trường, Siêu Trọng, Quá Khổ(182)'),
    ('487072', 'van-tai-hang-hoa-nguy-hiem', 'Vận Tải Hàng Hóa Nguy Hiểm(22)'),
    ('129310', 'xuat-nhap-khau-cac-cong-ty-xuat-nhap-khau', 'Xuất Nhập Khẩu - Các Công Ty Xuất Nhập Khẩu(1136)'),
    ('386770', 'kho-bai-cho-thue-kho-bai', 'Kho Bãi - Cho Thuê Kho Bãi(409)'),
    ('493219', 'dich-vu-van-chuyen-hang-hoa-trung-viet', 'Dịch Vụ Vận Chuyển Hàng Hóa Trung Việt(67)'),
    ('491551', 'chuyen-phat-nhanh-quoc-te-van-chuyen-hang-di-my-uc-anh-phap-canada,..', 'Chuyển Phát Nhanh Quốc Tế, Vận Chuyển Hàng Đi Mỹ, úc, Anh, Pháp, Canada,..(254)'),
    ('491554', 'chuyen-phat-nhanh-trong-nuoc', 'Chuyển Phát Nhanh Trong Nước(116)'),
    ('72610', 'buu-chinh-cac-dich-vu-buu-chinh', 'Bưu Chính - Các Dịch Vụ Bưu Chính(186)'),
    ('189130', 'buu-dien', 'Bưu Điện(54)'),
    ('492316', 'giao-hang-ship-hang-toan-quoc-dich-vu-(nhanh-tiet-kiem-thu-tien-ho)', 'Giao Hàng, Ship Hàng Toàn Quốc - Dịch Vụ (Nhanh, Tiết Kiệm Thu Tiền Hộ)(16)'),
    ('488353', 'tui-nilon-pe-tui-pp-tui-hdpe,..', 'Túi Nilon PE, Túi PP, Túi HDPE,..(849)'),
    ('488335', 'bao-pp-det', 'Bao PP Dệt(302)'),
    ('488359', 'mang-pvc-pet-pp-ps-hips', 'Màng PVC, PET, PP, PS, HIPS(295)'),
    ('490657', 'tui-bao-zipper-ziplock', 'Túi, Bao Zipper, Ziplock(264)'),
    ('488365', 'bao-bi-mang-ghep-bao-bi-mang-phuc-hop', 'Bao Bì Màng Ghép, Bao Bì Màng Phức Hợp(258)'),
    ('489124', 'in-bao-bi-nhua-in-mang-co-mang-ghep-in-ong-dong', 'In Bao Bì Nhựa, In Màng Co, Màng Ghép, In ống Đồng(257)'),
    ('490648', 'mang-pe-quan-pallet', 'Màng PE Quấn Pallet(249)'),
    ('489118', 'mang-phu-nong-nghiep-ngu-nghiep', 'Màng Phủ Nông Nghiệp, Ngư Nghiệp(126)'),
    ('488356', 'tui-phan-huy-sinh-hoc', 'Túi Phân Hủy Sinh Học(117)'),
    ('490468', 'thung-xop-khay-xop-hop-xop', 'Thùng Xốp, Khay Xốp, Hộp Xốp(101)'),
    ('488350', 'mang-bopp', 'Màng BOPP(68)'),
    ('489226', 'mang-boc-thuc-pham', 'Màng Bọc Thực Phẩm(65)'),
    ('488389', 'tam-pe-foam', 'Tấm PE Foam(38)'),
    ('490828', 'tui-pp-det', 'Túi PP Dệt(33)'),
    ('490474', 'tui-luoi-(tui-luoi-dung-trai-cay-thuc-pham)', 'Túi Lưới (Túi Lưới Đựng Trái Cây, Thực Phẩm)(29)'),
    ('490645', 'mang-nha-kinh', 'Màng Nhà Kính(29)'),
    ('492568', 'mang-cpp-mang-mcpp', 'Màng CPP, Màng MCPP(16)'),
    ('3092233', 'mang-ep-ly-san-xuat-thiet-ke-va-in-an', 'Màng ép Ly - Sản Xuất, Thiết Kế và In ấn(15)'),
    ('3092311', 'mang-eva-mang-peva', 'Màng EVA, Màng PEVA(9)'),
    ('488362', 'mang-pe-foam-(cuon-tam-tui-ong,.)-san-xuat-va-kinh-doanh', 'Màng PE Foam (Cuộn, Tấm, Túi, ống,.) - Sản Xuất Và Kinh Doanh(324)'),
    ('488221', 'bao-bi-pe-pp-hdpe-ldpe-(dang-tam-cuon-tui)', 'Bao Bì PE, PP, HDPE, LDPE (Dạng Tấm, Cuộn, Túi)(286)'),
    ('488074', 'bao-bi-nhua-dinh-hinh-khay-nhua-dinh-hinh', 'Bao Bì Nhựa Định Hình, Khay Nhựa Định Hình(264)'),
    ('487438', 'bao-container-bao-jumbo-bao-bigbag', 'Bao Container, Bao Jumbo, Bao Bigbag(159)'),
    ('492205', 'mang-in-chuyen-nhiet', 'Màng In Chuyển Nhiệt(28)'),
    ('486406', 'mang-pe-mang-chit-nha-san-xuat', 'Màng PE, Màng Chít - Nhà Sản Xuất(678)'),
    ('174170', 'bao-bi-nha-san-xuat-va-kinh-doanh', 'Bao Bì - Nhà Sản Xuất và Kinh Doanh(1096)'),
    ('488158', 'tui-vai-khong-det', 'Túi Vải Không Dệt(306)'),
    ('488368', 'tui-nhua-pvc', 'Túi Nhựa PVC(94)'),
    ('488341', 'tui-hut-chan-khong', 'Túi Hút Chân Không(48)'),
    ('492337', 'tui-uom-cay-tui-nhua-uom-cay', 'Túi Ươm Cây, Túi Nhựa Ươm Cây(12)'),
    ('489121', 'in-bao-bi-giay-(in-tui-giay-thung-giay-hop-giay)', 'In Bao Bì Giấy (In Túi Giấy, Thùng Giấy, Hộp Giấy)(922)'),
    ('488293', 'thung-carton-thung-giay', 'Thùng Carton, Thùng Giấy(801)'),
    ('488347', 'hop-giay-(hop-giay-carton-kraft,.)', 'Hộp Giấy (Hộp Giấy Carton, Kraft,.)(411)'),
    ('488338', 'tui-giay-tui-giay-kraft', 'Túi Giấy, Túi Giấy Kraft(320)'),
    ('488332', 'bao-giay-kraft', 'Bao Giấy Kraft(70)'),
    ('489220', 'vi-giay-khay-giay', 'Vỉ Giấy, Khay Giấy(50)'),
    ('3092272', 'lon-giay-hop-giay-tru-tron', 'Lon Giấy, Hộp Giấy Trụ Tròn(27)'),
    ('490876', 'thung-be-hop-be', 'Thùng Bế, Hộp Bế(17)'),
    ('492112', 'ke-giay-trung-bay', 'Kệ Giấy Trưng Bày(15)'),
    ('488071', 'ong-giay-loi-giay', 'ống Giấy, Lõi Giấy(203)'),
    ('484513', 'bao-bi-carton', 'Bao Bì Carton(689)'),
    ('492646', 'slip-sheets-san-xuat-va-cung-cap', 'Slip Sheets - Sản Xuất và Cung Cấp(23)'),
    ('489247', 'in-tui-nilon-in-tui-xop', 'In Túi Nilon, In Túi Xốp(137)'),
    ('191460', 'in-an-cong-ty-in-an-va-thiet-ke-(in-offset-in-flexo)', 'In ấn - Công ty In ấn Và Thiết Kế (In Offset, In Flexo)(2501)'),
    ('486127', 'quang-cao-thiet-ke-va-in-an-quang-cao-(in-ky-thuat-so-poster-hiflex-banner)', 'Quảng Cáo - Thiết Kế và In ấn Quảng Cáo (In Kỹ Thuật Số, Poster, Hiflex, Banner)(875)'),
    ('491410', 'gia-cong-sau-in-gia-cong-can-mang-ep-nhu-cat-can-be', 'Gia Công Sau In - Gia Công Cán Màng, ép Nhũ, Cắt, Cấn, Bế(97)'),
    ('136810', 'nhan-mac-kim-loai-tem-nhan-inox,.', 'Nhãn Mác Kim Loại, Tem Nhãn Inox,.(61)'),
    ('490168', 'thanh-ren-ty-ren-guzong', 'Thanh Ren, Ty Ren, Guzong(202)'),
    ('489964', 'bulong-neo-bulong-mong-bulong-luc-giac,..cac-loai', 'Bulong Neo, Bulong Móng, Bulong Lục Giác,..Các Loại(189)'),
    ('489958', 'bulong-inox-oc-inox-vit-inox', 'Bulong Inox, ốc Inox, Vít Inox(176)'),
    ('209520', 'vong-dem-long-den', 'Vòng Đệm, Long Đền(100)'),
    ('489973', 'bulong-no-vit-no-tac-ke', 'Bulong Nở, Vít Nở, Tắc Kê(90)'),
    ('492280', 'vit-ban-ton-vit-tu-khoan-vit-thach-cao,..cac-loai', 'Vít Bắn Tôn, Vít Tự Khoan, Vít Thạch Cao,..Các Loại(66)'),
    ('3092068', 'bulong-oc-vit-gia-cong-bulong-oc-vit-theo-yeu-cau', 'Bulong, ốc Vít - Gia Công Bulong, ốc Vít Theo Yêu Cầu(66)'),
    ('489967', 'dai-treo-ong-(hinh-tron-qua-bi-omega,..)', 'Đai Treo ống (Hình Tròn, Quả Bí, Omega,..)(60)'),
    ('489988', 'dinh-rut-dinh-tan-rive-nhom-rive-inox', 'Đinh Rút, Đinh Tán, Rive Nhôm, Rive Inox(42)'),
    ('489985', 'bulong-hoa-chat', 'Bulong Hóa Chất(27)'),
    ('493180', 'oc-vit-nho-oc-vit-sieu-nho', 'ốc Vít Nhỏ, ốc Vít Siêu Nhỏ(26)'),
    ('489970', 'kep-xa-go-(hinh-vuong-chu-c-kep-dam,..)', 'Kẹp Xà Gồ (Hình Vuông, Chữ C, Kẹp Dầm,..)(22)'),
    ('489955', 'dai-oc-e-cu-tan', 'Đai ốc, E Cu, Tán(185)'),
    ('493030', 'ban-le-(ban-le-inox-ban-le-cua,..-cac-loai)', 'Bản Lề (Bản Lề Inox, Bản Lề Cửa,.. Các Loại)(64)'),
    ('489601', 'vong-bi-nsk-bac-dan-nsk', 'Vòng Bi NSK/ Bạc Đạn NSK(105)'),
    ('489598', 'vong-bi-skf-bac-dan-skf', 'Vòng Bi SKF/ Bạc Đạn SKF(98)'),
    ('489616', 'vong-bi-koyo-bac-dan-koyo', 'Vòng Bi Koyo, Bạc Đạn Koyo(94)'),
    ('489646', 'goi-do-vong-bi-goi-do', 'Gối Đỡ, Vòng Bi Gối Đỡ(86)'),
    ('489610', 'vong-bi-cong-nghiep-bac-dan-cong-nghiep', 'Vòng Bi Công Nghiệp/ Bạc Đạn Công Nghiệp(81)'),
    ('489604', 'vong-bi-fag-bac-dan-fag', 'Vòng Bi FAG, Bạc Đạn FAG(69)'),
    ('489622', 'vong-bi-ntn-bac-dan-ntn', 'Vòng Bi NTN/ Bạc Đạn NTN(67)'),
    ('489631', 'vong-bi-timken-bac-dan-timken', 'Vòng Bi TIMKEN/ Bạc Đạn TIMKEN(46)'),
    ('489640', 'vong-bi-con', 'Vòng Bi Côn(45)'),
    ('489619', 'vong-bi-nachi--bac-dan-nachi', 'Vòng Bi Nachi / Bạc Đạn Nachi(37)'),
    ('489628', 'vong-bi-iko-bac-dan-iko', 'Vòng Bi IKO, Bạc Đạn IKO(26)'),
    ('489637', 'vong-bi-dua-bac-dan-dua', 'Vòng Bi Đũa, Bạc Đạn Đũa(21)'),
    ('489613', 'vong-bi-truot-bac-dan-truot', 'Vòng Bi Trượt/ Bạc Đạn Trượt(12)'),
    ('489625', 'vong-bi-fbj-bac-dan-fbj', 'Vòng Bi FBJ, Bạc Đạn FBJ(8)'),
    ('489643', 'vong-bi-chu-thap', 'Vòng Bi Chữ Thập(2)'),
    ('489607', 'vong-bi-lyc-bac-dan-lyc', 'Vòng Bi LYC, Bạc Đạn LYC(1)'),
    ('489634', 'bac-dan-ashi', 'Bạc Đạn ASHI(1)'),
    ('130210', 'cong-nghiep-may-moc-va-thiet-bi-cong-nghiep', 'Công Nghiệp - Máy Móc Và Thiết Bị Công Nghiệp(1121)'),
    ('331495', 'cong-nghiep-vat-tu-va-thiet-bi-cong-nghiep', 'Công nghiệp - Vật Tư và Thiết Bị Công Nghiệp(735)'),
    ('486124', 'tu-van-moi-truong-cong-ty-tu-van-moi-truong', 'Tư Vấn Môi trường - Công Ty Tư Vấn Môi Trường(425)'),
    ('255370', 'xu-ly-bui-xu-ly-khi-thai-thiet-bi-va-he-thong-xu-ly', 'Xử Lý Bụi, Xử Lý Khí Thải - Thiết Bị Và Hệ Thống Xử Lý(383)'),
    ('255510', 'xu-ly-chat-thai-xu-ly-rac-thai-dich-vu-thu-gom-va-xu-ly', 'Xử Lý Chất Thải, Xử Lý Rác Thải - Dịch Vụ Thu Gom Và Xử Lý(254)'),
    ('489373', 'quan-trac-moi-truong-dich-vu-quan-trac-moi-truong', 'Quan Trắc Môi Trường - Dịch Vụ Quan Trắc Môi Trường(142)'),
    ('3092281', 'tieu-huy-hang-hoa-dich-vu-tieu-huy-hang-hoa', 'Tiêu hủy hàng hóa - Dịch vụ tiêu hủy hàng hóa(35)'),
    ('349585', 'moi-truong-thiet-bi-moi-truong', 'Môi Trường - Thiết Bị Môi Trường(247)'),
    ('256260', 'nuoc-thau-cap-thoat-nuoc', 'Nước - Thầu Cấp, Thoát Nước(230)'),
    ('484489', 'hoa-chat-moi-truong', 'Hóa Chất Môi Trường(100)'),
    ('3092446', 'dau-thai-dich-vu-thu-gom-xu-ly-va-tai-che', 'Dầu Thải - Dịch Vụ Thu Gom, Xử Lý Và Tái Chế(23)'),
    ('491254', 'xu-ly-nuoc-thai-cong-nghiep-(det-nhuom-nganh-giay,..nha-may)', 'Xử Lý Nước Thải Công Nghiệp (Dệt Nhuộm, Ngành Giấy,..Nhà Máy)(178)'),
    ('491251', 'xu-ly-nuoc-thai-sinh-hoat-(khu-dan-cu-do-thi-gia-dinh,.)', 'Xử Lý Nước Thải Sinh Hoạt (Khu Dân Cư, Đô Thị, Gia Đình,.)(146)'),
    ('491266', 'xu-ly-nuoc-cap-nuoc-sach-(nuoc-gieng-khoan-nhiem-man-phen,..)', 'Xử Lý Nước Cấp, Nước Sạch (Nước Giếng Khoan, Nhiễm Mặn, Phèn,..)(91)'),
    ('491257', 'xu-ly-nuoc-thai-benh-vien-xu-ly-nuoc-thai-y-te', 'Xử Lý Nước Thải Bệnh Viện, Xử Lý Nước Thải Y Tế(50)'),
    ('484357', 'hoa-chat-xu-ly-nuoc-nuoc-thai-(chat-tro-lang-pac-chloramin-b-phen-nhom,.)', 'Hóa Chất Xử Lý Nước - Nước Thải (Chất Trợ Lắng PAC, Chloramin B, Phèn Nhôm,.)(423)'),
    ('488575', 'nuoc-nuoc-thai-thiet-bi-xu-ly-nuoc-nuoc-thai', 'Nước, Nước Thải - Thiết Bị Xử Lý Nước, Nước Thải(258)'),
    ('488569', 'vat-lieu-loc-nuoc-(cat-soi-hat-than,.)', 'Vật Liệu Lọc Nước (Cát, Sỏi, Hạt, Than,.)(213)'),
    ('337525', 'may-mac-quan-ao-thoi-trang', 'May Mặc - Quần áo Thời Trang(467)'),
    ('48860', 'quan-ao-tre-em-san-xuat-va-ban-buon-quan-ao-tre-em', 'Quần áo Trẻ Em - Sản Xuất và Bán Buôn Quần áo Trẻ Em(305)'),
    ('488404', 'may-xuat-khau-cong-ty-may-quan-ao-xuat-khau', 'May Xuất Khẩu - Công Ty May Quần áo Xuất Khẩu(268)'),
    ('250760', 'do-lot-may-do-lot-va-ban-buon-do-lot', 'Đồ Lót - May Đồ Lót Và Bán Buôn Đồ Lót(182)'),
    ('232910', 'may-xuat-khau-thoi-trang-nu', 'May Xuất Khẩu - Thời Trang Nữ(165)'),
    ('61060', 'may-do-va-thiet-ke-thoi-trang-(vay-cuoi-ao-dai-dam-da-hoi,..)', 'May Đo Và Thiết Kế Thời Trang (Váy Cưới, áo Dài, Đầm Dạ Hội,..)(134)'),
    ('488713', 'may-quang-cao-nhan-hop-dong-may-quang-cao-theo-yeu-cau', 'May Quảng Cáo - Nhận Hợp Đồng May Quảng Cáo Theo Yêu Cầu(91)'),
    ('288280', 'may-san-quan-kaki-quan-jean', 'May Sẵn - Quần Kaki, Quần Jean(84)'),
    ('488902', 'may-xuat-khau-quan-ao-tre-em', 'May Xuất Khẩu - Quần áo Trẻ Em(45)'),
    ('489253', 'vay-ao-khau-trang-chong-nang-san-xuat-va-ban-buon', 'Váy, áo, Khẩu Trang Chống Nắng - Sản Xuất Và Bán Buôn(17)'),
    ('487309', 'may-xuat-khau-dich-vu-san-xuat-va-gia-cong-(theo-don-dat-hang)', 'May Xuất Khẩu - Dịch Vụ Sản Xuất Và Gia Công (Theo Đơn Đặt Hàng)(270)'),
    ('487303', 'may-mac-quan-ao-the-thao', 'May Mặc - Quần áo Thể Thao(91)'),
    ('487297', 'may-mac-do-ngu-thoi-trang-mac-nha', 'May Mặc - Đồ Ngủ, Thời Trang Mặc Nhà(63)'),
    ('487300', 'may-mac-thoi-trang-khieu-vu-da-hoi', 'May Mặc - Thời Trang Khiêu Vũ, Dạ Hội(12)'),
    ('61210', 'quan-ao-ban-buon-ban-si-quan-ao', 'Quần áo - Bán Buôn, Bán Sỉ Quần áo(775)'),
    ('22310', 'balo-tui-xach-cap-gio-vali--cong-ty-may-va-san-xuat-balo-tui-xach', 'Balo, Túi Xách, Cặp, Giỏ, Vali -  Công Ty May và Sản Xuất Balo, Túi Xách(664)'),
    ('214810', 'giay-dep-giay-da-san-xuat-va-ban-buon', 'Giày Dép, Giày Da - Sản Xuất và Bán Buôn(648)'),
    ('122460', 'non-mu-san-xuat-va-ban-buon', 'Nón, Mũ - Sản Xuất và Bán Buôn(267)'),
    ('136110', 'det-kim-san-pham', 'Dệt Kim - Sản Phẩm(155)'),
    ('488407', 'det-may-cac-cong-ty-det-may', 'Dệt May - Các Công Ty Dệt May(80)'),
    ('492106', 'vai-xep-ly-dich-vu-gia-cong-xep-ly-cat-vien-vai', 'Vải Xếp Ly - Dịch Vụ Gia Công Xếp Ly, Cắt Viền Vải(67)'),
    ('488905', 'sua-loi-vai-dich-vu-sua-loi-vai', 'Sửa Lỗi vải - Dịch Vụ Sửa Lỗi Vải(39)'),
    ('489340', 'ao-thun-dong-phuc-cong-ty-may-ao-thun-ao-phong-dong-phuc', 'áo Thun Đồng Phục - Công Ty May áo Thun, áo Phông Đồng Phục(644)'),
    ('485440', 'dong-phuc-cong-so-dong-phuc-van-phong-cong-ty,.', 'Đồng Phục Công Sở, Đồng Phục Văn Phòng, Công Ty,.(585)'),
    ('485428', 'dong-phuc-hoc-sinh-may-dong-phuc-hoc-sinh-hoc-sinh-tieu-hoc', 'Đồng Phục Học Sinh, May Đồng Phục Học Sinh, Học Sinh Tiểu Học(429)'),
    ('490090', 'in-ao-thun-ao-phong-in-ao-dong-phuc', 'In áo Thun, áo Phông, In áo Đồng Phục(150)'),
    ('489205', 'dong-phuc-bao-ve-(quan-ao-bao-ve-ao-bao-ve-quan-ao-bao-ve-may-san)', 'Đồng Phục Bảo Vệ (Quần áo Bảo Vệ, áo Bảo Vệ, Quần áo Bảo Vệ May Sẵn)(148)'),
    ('490087', 'dong-phuc-lop-ao-dong-phuc-lop', 'Đồng Phục Lớp, áo Đồng Phục Lớp(87)'),
    ('485431', 'dong-phuc-the-thao', 'Đồng Phục Thể Thao(75)'),
    ('490081', 'dong-phuc-mam-non-mau-giao-dong-phuc-giao-vien-mam-non', 'Đồng Phục Mầm Non, Mẫu Giáo, Đồng Phục Giáo Viên Mầm Non(61)'),
    ('490084', 'ao-dong-phuc', 'áo Đồng Phục(51)'),
    ('492307', 'dong-phuc-spa', 'Đồng Phục Spa(48)'),
    ('490093', 'dong-phuc-gia-dinh-ao-dong-phuc-gia-dinh', 'Đồng Phục Gia Đình, áo Đồng Phục Gia Đình(27)'),
    ('3092095', 'dong-phuc-su-kien-ao-thun-su-kien', 'Đồng Phục Sự Kiện, áo Thun Sự Kiện(21)'),
    ('487330', 'dong-phuc-nha-hang-khach-san-(ao-non-bep-tap-de-dong-phuc-buong-phong,.)', 'Đồng Phục Nhà Hàng, Khách Sạn (áo, Nón Bếp, Tạp Dề, Đồng Phục Buồng Phòng,.)(311)'),
    ('487333', 'dong-phuc-cong-nhan-may-dong-phuc-ao-quan-ao-cong-nhan', 'Đồng Phục Công Nhân - May Đồng Phục, áo, Quần áo Công Nhân(211)'),
    ('487336', 'dong-phuc-benh-vien-dong-phuc-y-te', 'Đồng Phục Bệnh Viện, Đồng Phục Y tế(188)'),
    ('484405', 'quan-ao-bao-ho-lao-dong', 'Quần áo Bảo Hộ Lao Động(797)'),
    ('487324', 'ao-phong-ao-thun-san-xuat-va-ban-buon', 'áo Phông, áo Thun - Sản Xuất Và Bán Buôn(242)'),
    ('492535', 'dong-phuc-so-mi-dong-phuc-ao-so-mi', 'Đồng Phục Sơ Mi, Đồng Phục áo Sơ Mi(86)'),
    ('492916', 'co-to-quoc-co-le-hoi-co-phat-giao-xuong-may-va-san-xuat-theo-yeu-cau', 'Cờ Tổ Quốc, Cờ Lễ Hội, Cờ Phật Giáo - Xưởng May và Sản Xuất Theo Yêu Cầu(36)'),
    ('485434', 'in-logo-in-phu-hieu-tren-ao', 'In Logo, In Phù Hiệu Trên áo(35)'),
    ('492559', 'quan-au-nam-quan-tay-nam', 'Quần Âu Nam, Quần Tây Nam(25)'),
    ('261310', 'day-dai-det-(phu-lieu-may-mac-giay-dep-balo,.)', 'Dây Đai Dệt (Phụ Liệu May Mặc, Giày Dép, Balo,.)(215)'),
    ('264160', 'day-khoa-keo-dau-khoa-keo-san-xuat-va-ban-buon', 'Dây Khoá Kéo, Đầu Khóa Kéo - Sản Xuất và Bán Buôn(155)'),
    ('39360', 'khuy-nut-cuc-(kim-loai-nhua-go)-san-xuat-va-ban-buon', 'Khuy, Nút, Cúc (Kim Loại, Nhựa, Gỗ) - Sản Xuất và Bán Buôn(149)'),
    ('490783', 'mex-keo-dung-(giay-vai)', 'Mex, Keo, Dựng (Giấy, Vải)(124)'),
    ('490774', 'vai-lot-(vai-lot-tui-tui-xach-ao-gio,.)', 'Vải Lót (Vải Lót Túi, Túi Xách, áo Gió,.)(78)'),
    ('492124', 'bo-co-ao-bo-co-ao-thun-bo-tay-ao', 'Bo Cổ áo, Bo Cổ áo Thun, Bo Tay áo(66)'),
    ('492175', 'phu-lieu-kim-loai-may-mac-(khoen-khoa-ode-ri-ve-moc-nep,..)', 'Phụ Liệu Kim Loại May Mặc (khoen, khóa, ode, ri ve, móc, nẹp,..)(64)'),
    ('492547', 'day-treo-nhan-mac-day-treo-the-bai-ti-xo', 'Dây Treo Nhãn Mác, Dây Treo Thẻ Bài, Ti Xỏ(59)'),
    ('492274', 'day-ruy-bang-day-ribbon', 'Dây Ruy Băng, Dây Ribbon(56)'),
    ('492172', 'phu-lieu-dong-goi-may-mac-san-xuat-va-cung-cap', 'Phụ Liệu Đóng Gói May Mặc - Sản Xuất Và Cung Cấp(48)'),
    ('490777', 'dem-vai-dem-nguc', 'Đệm Vai, Đệm Ngực(43)'),
    ('489751', 'vai-luoi-tricot-(vai-phu-lieu-may-mac)', 'Vải Lưới Tricot (Vải Phụ Liệu May Mặc)(34)'),
    ('492202', 'bang-nham-dinh-xe-velcro', 'Băng Nhám Dính, Xé, Velcro(30)'),
    ('492421', 'day-chong-bai-day-moblion-tape', 'Dây Chống Bai, Dây MobLion Tape(18)'),
    ('492271', 'day-se-day-tim-day-cord', 'Dây Se, Dây Tim, Dây Cord(14)'),
    ('491608', 'phu-lieu-may-mac-nhua-(day-nhua-khoa-nhua-khoen-nhua,.)', 'Phụ Liệu May Mặc Nhựa (Dây Nhựa, Khóa Nhựa, Khoen Nhựa,.)(39)'),
    ('488308', 'day-thun-det-(thun-tron-thun-ban-dai-thun,.)', 'Dây Thun Dệt (Thun Tròn, Thun Bản, Đai Thun,.)(207)'),
    ('97160', 'vai-soi-san-xuat-va-kinh-doanh', 'Vải Sợi - Sản Xuất và Kinh Doanh(734)'),
    ('488791', 'nhan-mac-quan-ao-nhan-mac-may-mac', 'Nhãn Mác Quần áo, Nhãn Mác May Mặc(262)'),
    ('239660', 'chi-may-va-chi-theu', 'Chỉ May và Chỉ Thêu(207)'),
    ('72060', 'bong-gon-cong-nghiep-gon-bi-gon-tam-gon-cuon,', 'Bông Gòn Công Nghiệp - Gòn Bi, Gòn Tấm, Gòn Cuộn,(148)'),
    ('488857', 'moc-ao-moc-treo-quan-ao-va-cac-phu-kien,..-san-xuat-va-kinh-doanh', 'Móc áo, Móc Treo Quần áo Và Các Phụ Kiện,.. - Sản Xuất Và Kinh Doanh(107)'),
    ('491422', 'da-thuoc-(da-ca-sau-da-bo-trau-de,..)-nguyen-lieu-da-that', 'Da Thuộc (Da Cá Sấu, Da Bò, Trâu, Dê,..) - Nguyên Liệu Da Thật(63)'),
    ('98260', 'long-long-vu-nguyen-lieu-va-san-pham', 'Lông, Lông Vũ - Nguyên Liệu Và Sản Phẩm(32)'),
    ('492211', 'gia-cong-dan-vai-dich-vu-gia-cong-can-dan-boi-vai', 'Gia Công Dán Vải - Dịch Vụ Gia Công Cán, Dán, Bồi Vải(28)'),
    ('488206', 'co-khi-che-tao-may', 'Cơ Khí - Chế Tạo Máy(526)'),
    ('488371', 'do-ga-(do-ga-lap-rap-kiem-tra-van-nang,..)', 'Đồ Gá (Đồ Gá Lắp Ráp, Kiểm Tra, Vạn Năng,..)(186)'),
    ('492301', 'co-khi-dich-vu-gia-cong-cat-chan-dot-dap-co-khi', 'Cơ Khí - Dịch Vụ Gia Công Cắt, Chấn, Đột Dập Cơ Khí(181)'),
    ('485179', 'co-khi-dan-dung-(tu-tai-lieu-giuong-tang-ban-ghe,..)', 'Cơ Khí Dân Dụng (Tủ Tài Liệu, Giường Tầng, Bàn Ghế,..)(117)'),
    ('488374', 'banh-rang-(banh-rang-tru-con-xoan,..)', 'Bánh Răng (Bánh Răng Trụ, Côn, Xoắn,..)(76)'),
    ('492073', 'han-co-khi-dich-vu-han-laser-gio-da-ho-quang-dien,.', 'Hàn Cơ Khí - Dịch Vụ Hàn Laser, Gió Đá, Hồ Quang Điện,.(40)'),
    ('486193', 'can-bang-dong-dich-vu-can-bang-dong', 'Cân Bằng Động - Dịch Vụ Cân Bằng Động(37)'),
    ('492403', 'ren-dap-nong-chi-tiet-linh-kien-chinh-xac-theo-yeu-cau', 'Rèn, Dập Nóng - Chi Tiết, Linh Kiện Chính Xác Theo Yêu Cầu(26)'),
    ('492298', 'co-khi-moi-truong-(bon-nuoc-thai-be-lang-lo-dot-chat-thai,.)', 'Cơ Khí Môi Trường (Bồn Nước Thải, Bể Lắng, Lò Đốt Chất Thải,.)(23)'),
    ('492295', 'co-khi-vien-thong-(tru-thap-anten-cot-vien-thong-nha-tram,..)', 'Cơ Khí Viễn Thông (Trụ Tháp Anten, Cột Viễn Thông, Nhà Trạm,..)(18)'),
    ('492292', 'co-khi-thuy-dien-thuy-loi-(cua-van-cung-gau-vot-rac-cua-xa-nuoc,.)', 'Cơ Khí Thủy Điện, Thủy Lợi (Cửa Van Cung, Gầu Vớt Rác, Cửa Xả Nước,.)(14)'),
    ('489277', 'co-khi-san-khau-thiet-ke-thi-cong-va-lap-dat', 'Cơ Khí Sân Khấu - Thiết Kế, Thi Công Và Lắp Đặt(8)'),
    ('486703', 'co-khi-dich-vu-phuc-hoi-chi-tiet-may-va-khac,..', 'Cơ Khí - Dịch Vụ Phục Hồi Chi Tiết Máy Và Khác,..(46)'),
    ('486706', 'co-khi-gia-cong-co-khi-theo-yeu-cau', 'Cơ Khí - Gia Công Cơ Khí Theo Yêu Cầu(645)'),
    ('486697', 'co-khi-xay-dung-(vi-keo-thep-nha-khung-thep-ket-cau-nha-xuong,..)', 'Cơ Khí Xây Dựng (Vì Kèo Thép, Nhà Khung Thép, Kết Cấu Nhà Xưởng,..)(293)'),
    ('486709', 'gia-cong-kim-loai-tam-(han-chan-dot-dap,..tam-kim-loai)', 'Gia Công Kim Loại Tấm (Hàn, Chấn, Đột Dập,..Tấm Kim Loại)(283)'),
    ('486700', 'co-khi-cong-nghiep-(bang-chuyen-duong-ong,..)', 'Cơ Khí Công Nghiệp (Băng Chuyền, Đường ống,..)(138)'),
    ('69810', 'bang-tai-cac-cong-ty-bang-tai', 'Băng Tải - Các Công Ty Băng Tải(562)'),
    ('227515', 'ke-chua-hang-ke-cong-nghiep-(ke-kho-hang-sat-thep,..)', 'Kệ Chứa Hàng, Kệ Công Nghiệp (Kệ Kho Hàng, Sắt, Thép,..)(361)'),
    ('106960', 'duc-chi-tiet-dong-nhom-sat-thep-(chinh-xac-theo-yeu-cau)', 'Đúc Chi Tiết - Đồng, Nhôm, Sắt, Thép (Chính Xác Theo Yêu Cầu)(330)'),
    ('233560', 'bon-chua-bon-chua-hoa-chat-xang-dau-ap-luc-cong-nghiep,.', 'Bồn Chứa - Bồn Chứa Hóa Chất, Xăng Dầu, áp Lực, Công Nghiệp,.(265)'),
    ('488833', 'day-chuyen-san-xuat-thiet-ke-va-che-tao', 'Dây Chuyền Sản Xuất - Thiết Kế Và Chế Tạo(155)'),
    ('492580', 'ban-thao-tac-ban-lap-rap', 'Bàn Thao Tác, Bàn Lắp Ráp(94)'),
    ('3092458', 'gia-cong-cnc-gia-cong-cnc-theo-yeu-cau', 'Gia Công CNC, Gia Công CNC Theo Yêu Cầu(55)'),
    ('3092461', 'linh-kien-co-khi-san-xuat-oem-odm', 'Linh Kiện Cơ Khí - Sản Xuất OEM, ODM(31)'),
    ('163010', 'phu-tung-o-to-phu-tung-xe-hoi', 'Phụ Tùng Ô Tô, Phụ Tùng Xe Hơi(1132)'),
    ('164160', 'phu-tung-xe-may-linh-phu-kien-xe-may', 'Phụ Tùng Xe Máy, Linh Phụ Kiện Xe Máy(629)'),
    ('342550', 'phu-kien-cua-(cua-kinh-nhua-go-thep,..)', 'Phụ Kiện Cửa (Cửa Kính, Nhựa, Gỗ, Thép,..)(427)'),
    ('486199', 'phu-kien-nganh-go-phu-kien-noi-that-san-xuat-va-cung-cap', 'Phụ Kiện Ngành Gỗ, Phụ Kiện Nội Thất - Sản Xuất và Cung Cấp(358)'),
    ('488086', 'phu-kien-duong-ong-te-co-giam-cut,..thep-gang-dong-nhom-kim-loai', 'Phụ Kiện Đường ống - Tê, Co, Giảm, Cút,..Thép, Gang, Đồng, Nhôm, Kim Loại(265)'),
    ('113070', 'bep-gas-linh-phu-kien-bep-gas', 'Bếp Gas - Linh, Phụ Kiện Bếp Gas(121)'),
    ('28335', 'phu-tung-xe-dap-phu-kien-xe-dap', 'Phụ Tùng Xe Đạp, Phụ Kiện Xe Đạp(96)'),
    ('493183', 'duc-ap-luc-gia-cong-duc-ap-luc-kim-loai-(nhom-dong-kem,..)', 'Đúc áp Lực - Gia Công Đúc áp Lực Kim Loại (Nhôm, Đồng, Kẽm,..)(74)'),
    ('490252', 'khuon-ep-nhua', 'Khuôn ép Nhựa(185)'),
    ('154460', 'khuon-mau-linh-phu-kien-khuon-mau', 'Khuôn Mẫu - Linh Phụ Kiện Khuôn Mẫu(115)'),
    ('490243', 'khuon-nhua-dinh-hinh-khuon-hut-chan-khong', 'Khuôn Nhựa Định Hình, Khuôn Hút Chân Không(109)'),
    ('490249', 'khuon-thoi-khuon-thoi-nhua', 'Khuôn Thổi, Khuôn Thổi Nhựa(31)'),
    ('3092431', 'khuon-dun-khuon-dun-cong-nghiep', 'Khuôn Đùn, Khuôn Đùn Công Nghiệp(7)'),
    ('485098', 'khuon-nhua-khuon-nhua-cong-nghiep', 'Khuôn Nhựa, Khuôn Nhựa Công Nghiệp(163)'),
    ('485113', 'khuon-duc-khuon-duc-ap-luc', 'Khuôn Đúc, Khuôn Đúc áp Lực(127)'),
    ('488314', 'khuon-be-khuon-dao-dap', 'Khuôn Bế, Khuôn Dao Dập(112)'),
    ('485107', 'khuon-dap-(dap-nong-lanh-vuot-lien-hoan)', 'Khuôn Dập (Dập Nóng, Lạnh, Vuốt, Liên Hoàn)(126)'),
    ('488926', 'xu-ly-nhiet-gia-cong-xu-ly-nhiet-(chan-khong-toi-cung-tham-carbon,.)', 'Xử Lý Nhiệt - Gia Công Xử Lý Nhiệt (Chân Không, Tôi Cứng, Thấm Carbon,.)(96)'),
    ('490258', 'khuon-cao-su', 'Khuôn Cao Su(28)'),
    ('492928', 'khuon-chau-canh-(khuon-chau-abs-composite-xi-mang,.)', 'Khuôn Chậu Cảnh (Khuôn Chậu ABS, Composite, Xi Măng,.)(28)'),
    ('492616', 'khuon-be-tong-ly-tam-(coc-ong-cong-hang-rao,..)', 'Khuôn Bê Tông Ly Tâm (cọc, ống cống, hàng rào,..)(18)'),
    ('493195', 'xu-ly-be-mat-khuon-mau-(danh-bong-tao-nham-an-mon-hoa-van,..)', 'Xử Lý Bề Mặt Khuôn Mẫu (Đánh Bóng, Tạo Nhám, Ăn Mòn Hoa Văn,..)(16)'),
]

PROVINCES = [
    'tp.-hồ-chí-minh-(tphcm)',
    'đồng-nai',
    'bình-dương',
    'tp.-đà-nẵng',
    'tp.-hải-phòng',
    'an-giang',
    'bà-rịa-vũng-tàu',
    'bắc-ninh',
    'bình-phước',
    'bình-thuận',
    'hưng-yên',
    'khánh-hòa',
    'nam-định',
    'phú-thọ',
    'quảng-ninh',
    'thái-nguyên',
    'thanh-hóa',
    'thừa-thiên-huế',
    'tp.-cần-thơ',
    'vĩnh-phúc',
    'bắc-giang',
    'bình-định',
    'gia-lai',
    'hà-nam',
    'hải-dương',
    'long-an',
    'ninh-bình',
    'ninh-thuận',
    'quảng-ngãi',
    'tây-ninh',
    'hà-tĩnh',
    'lạng-sơn',
    'nghệ-an',
    'đắk-lắk',
    'bến-tre',
    'cao-bằng',
    'lâm-đồng',
    'lào-cai',
    'hòa-bình',
    'quảng-trị',
    'phú-yên',
    'đồng-tháp',
    'tuyên-quang',
    'thái-bình',
    'sơn-la',
    'vĩnh-long',
    'trà-vinh',
    'quảng-nam',
    'tiền-giang',
    'bạc-liêu',
    'hà-giang',
    'yên-bái',
    'kiên-giang',
    'hậu-giang',
    'kon-tum',
    'sóc-trăng',
    'quảng-bình',
    'cà-mau',
    'lai-châu',
    'đắk-nông',
]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9",
        "Referer": BASE_URL + "/",
    }

def clean(text) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()

def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=20,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )

def build_url(cat_id: str, cat_slug: str, prov_slug: str) -> str:
    """
    /cateprovinces/{cat_id}/{cat_slug}-ở-tại-{prov_slug}.html
    Encode unicode để tránh lỗi HTTP.
    """
    path = f"/cateprovinces/{cat_id}/{cat_slug}-ở-tại-{prov_slug}.html"
    return BASE_URL + quote(path, safe="/:.-")


def load_progress() -> tuple[set, set]:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            data = json.load(f)
        done   = set(tuple(x) for x in data.get("done", []))
        visited = set(data.get("visited", []))
        print(f"Resume: {len(done):,} combos done, {len(visited):,} URLs visited")
        return done, visited
    return set(), set()

def save_progress(done: set, visited: set):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"done": list(done), "visited": list(visited)}, f)


async def fetch(client: httpx.AsyncClient, url: str) -> Optional[str]:
    for attempt in range(3):
        try:
            r = await client.get(url, headers=get_headers())
            if r.status_code == 200:
                return r.text
            if r.status_code in (403, 404):
                return None          # không retry
            await asyncio.sleep(1 * (attempt + 1))
        except Exception as e:
            if attempt == 2:
                print(f"  ✗ FETCH FAIL {url}: {e}")
            await asyncio.sleep(1)
    return None


# ================= LISTING CRAWLER =================
async def fetch_all_links(client: httpx.AsyncClient, start_url: str) -> List[str]:
    """Crawl tất cả trang pagination của 1 URL, trả về listing links."""
    all_links: set = set()
    page = 1
    while True:
        url = f"{start_url}?page={page}" if page > 1 else start_url
        html = await fetch(client, url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/listings/" in href:
                if href.startswith("/"):
                    href = BASE_URL + href
                links.add(href)

        if not links or links.issubset(all_links):
            break   # hết trang hoặc trang lặp

        all_links.update(links)
        page += 1
        await asyncio.sleep(0.3)

    return list(all_links)


def extract_json_ld(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {}

def extract_label_block(soup: BeautifulSoup, label: str) -> str:
    for block in soup.select("div.p-2.clearfix"):
        span = block.find("span")
        if span and label.lower() in span.get_text(strip=True).lower():
            full = clean(block.get_text(" ", strip=True))
            return clean(full.replace(clean(span.get_text(strip=True)), "").lstrip(":"))
    return ""

def get_address(soup: BeautifulSoup) -> str:
    label = soup.find(string=lambda t: t and "đ/c" in t.lower())
    if not label:
        return ""
    node = label.next_sibling
    while node and not isinstance(node, NavigableString):
        node = node.next_sibling
    return clean(str(node)) if node else ""

def normalize_emp(text: str) -> str:
    if not text:
        return ""
    if "ít hơn" in text.lower(): return "0-10"
    if "trên" in text.lower():   return "500+"
    m = re.search(r"(\d+)\s*-\s*(\d+)", text)
    return f"{m.group(1)}-{m.group(2)}" if m else clean(text)

def extract_district(address: str) -> str:
    if not address:
        return ""
    m = re.search(r"(Q\.|H\.|TP\.|Quận|Huyện|Thành phố)\s*[^,]+", address)
    return m.group(0).strip() if m else ""

def parse_company(html: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")
    schema = extract_json_ld(soup)

    if schema:
        addr   = schema.get("address", {})
        addr   = addr if isinstance(addr, dict) else {}
        street = addr.get("streetAddress", "")
        region = addr.get("addressRegion", "")
        raw_emp = schema.get("numberOfEmployees", {})
        emp    = raw_emp.get("value", "") if isinstance(raw_emp, dict) else str(raw_emp)
        industries = schema.get("knowsAbout", [])

        tax_code = (schema.get("taxID", "") or extract_label_block(soup, "MÃ SỐ THUẾ")).strip()
        name     = schema.get("name", "") or ""

        if not tax_code and not name:
            return None

        return {
            "tax_code":     tax_code,
            "name":         name,
            "founded_year": str(schema.get("foundingDate", "") or extract_label_block(soup, "NĂM THÀNH LẬP")),
            "address":      clean(f"{street} {region}") or get_address(soup),
            "district":     extract_district(street),
            "employee_size": normalize_emp(emp) or normalize_emp(extract_label_block(soup, "SỐ LƯỢNG NHÂN VIÊN")),
            "industry":     ", ".join(industries) if isinstance(industries, list) else str(industries),
        }

    # Fallback HTML
    name_el  = soup.select_one(".noidung_chantrang span") or soup.find("h1")
    name     = clean(name_el.get_text()) if name_el else ""
    tax_code = extract_label_block(soup, "MÃ SỐ THUẾ").strip()
    address  = get_address(soup)

    if not tax_code and not name:
        return None

    industries = []
    for block in soup.select("div.p-2.clearfix"):
        span = block.find("span")
        if span and "ngành nghề" in span.get_text(strip=True).lower():
            industries = [a.get_text(strip=True) for a in block.find_all("a")]
            break

    return {
        "tax_code":     tax_code,
        "name":         name,
        "founded_year": extract_label_block(soup, "NĂM THÀNH LẬP"),
        "address":      address,
        "district":     extract_district(address),
        "employee_size": normalize_emp(extract_label_block(soup, "SỐ LƯỢNG NHÂN VIÊN")),
        "industry":     ", ".join(industries),
    }


# ================= DB =================
async def flush_to_db(buffer: list):
    if not buffer:
        return

    # Global dedup trước khi insert
    seen, unique = set(), []
    for r in buffer:
        tc = (r.get("tax_code") or "").strip()
        if tc and tc not in seen:
            seen.add(tc)
            unique.append(r)

    buffer.clear()
    skipped = len(buffer) - len(unique) if False else 0

    inserted = 0
    for i in range(0, len(unique), BATCH_SIZE):
        batch = unique[i:i + BATCH_SIZE]
        try:
            supabase.table(TABLE).upsert(batch, on_conflict="tax_code").execute()
            inserted += len(batch)
        except Exception as e:
            print(f"  ✗ DB error: {e}")

    print(f"  💾 {inserted} inserted, {len(unique) - inserted} failed, {len(buffer)} no tax_code skipped")


# ================= WORKER =================
async def process_detail(url: str, client: httpx.AsyncClient, buffer: list, sem: asyncio.Semaphore):
    async with sem:
        html = await fetch(client, url)
    if not html:
        return
    data = parse_company(html)
    if data:
        buffer.append(data)


# ================= MAIN =================
async def main():
    done_combos, visited_urls = load_progress()
    total = len(CATEGORIES) * len(PROVINCES)
    remaining = total - len(done_combos)
    print(f"\n📊 {len(CATEGORIES)} categories × {len(PROVINCES)} tỉnh = {total:,} combos")
    print(f"   Done: {len(done_combos):,} | Remaining: {remaining:,}\n")

    result_buffer: list = []
    sem = asyncio.Semaphore(DETAIL_WORKERS)

    async with make_client() as client:
        for cat_id, cat_slug, cat_name in CATEGORIES:
            for prov_slug in PROVINCES:
                combo = (cat_id, prov_slug)
                if combo in done_combos:
                    continue

                url = build_url(cat_id, cat_slug, prov_slug)
                print(f"\n▶ {cat_name} × {prov_slug}")

                # Lấy tất cả listing links của combo này
                links = await fetch_all_links(client, url)
                new_links = [l for l in links if l not in visited_urls]

                print(f"  {len(new_links)} new / {len(links)} total links")

                if new_links:
                    queue: asyncio.Queue = asyncio.Queue()
                    for link in new_links:
                        await queue.put(link)

                    async def worker():
                        while True:
                            detail_url = await queue.get()
                            if detail_url is None:
                                queue.task_done()
                                break
                            try:
                                await process_detail(detail_url, client, result_buffer, sem)
                                visited_urls.add(detail_url)
                            finally:
                                queue.task_done()

                    tasks = [asyncio.create_task(worker()) for _ in range(DETAIL_WORKERS)]
                    await queue.join()
                    for _ in range(DETAIL_WORKERS):
                        await queue.put(None)
                    await asyncio.gather(*tasks)

                # Đánh dấu combo done + save progress
                done_combos.add(combo)
                save_progress(done_combos, visited_urls)

                # Flush DB sau mỗi combo (nếu đủ batch)
                if len(result_buffer) >= BATCH_SIZE:
                    await flush_to_db(result_buffer)

        # Flush phần còn lại cuối cùng
        if result_buffer:
            print(f"\n💾 Final flush {len(result_buffer)} records...")
            await flush_to_db(result_buffer)

    print(f"\n🏁 DONE — {len(visited_urls):,} URLs crawled, {len(done_combos):,} combos finished")


if __name__ == "__main__":
    asyncio.run(main())