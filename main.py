from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import random
import nltk
import mysql.connector
import ssl

# konksi nanti coba ditaruh diluar
# koneksi
db = mysql.connector.connect(
    host='sql6.freemysqlhosting.net',
    user='sql6459951',
    password='nJChTVrmVU',
    database='sql6459951',
    port=3306
    )
cursor = db.cursor()

class Scrape:
    def __init__(self):
        self.allKey = Scrape.getAllKeyword()
        random.shuffle(self.allKey)
        for key in self.allKey:
            print("-------------------------------------------------------")
            print("Use " + key[0] + " for keyword")
            print("-------------------------------------------------------")
            Scrape.scrapeMain(key[0])

    def getAllKeyword():
        cursor.execute("SELECT * FROM keyword")
        allKeyword = cursor.fetchall()
        
        return allKeyword

    def scrapeMain(keyword):
        # variable untuk hitung
        add = 0
        exist = 0

        # loop scrape
        cursor.execute("SELECT * FROM scrape")
        raw_scrape = cursor.fetchall()
        random.shuffle(raw_scrape)
        random.shuffle(raw_scrape)
        for rowS in raw_scrape:
            asal = rowS[0]
            # scrape data
            main_link1 = rowS[1]
            main_link2 = rowS[2]
            main_link = main_link1 + keyword + main_link2
            tag_main = rowS[3]
            tag_lowongan = rowS[4]
            tag_lowongan_part = rowS[5]
            tag_perusahaan = rowS[6]
            tag_perusahaan_part = rowS[7]
            tag_lokasi = rowS[8]
            tag_lokasi_part = rowS[9]
            tag_mainDetail = rowS[10]
            tag_keterangan = rowS[11]
            tag_skill = rowS[12]
            tag_benefit = rowS[13]
            tag_deskripsi = rowS[14]
            breakDeskripsi = rowS[15]
            raw_link = rowS[16]

            print("Scrape " + asal + " on progress")

            try:
                r = Request(main_link, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'})
                gcontext = ssl.SSLContext()  # Only for gangstars
                response = urlopen(r, context=gcontext).read()
                soup = BeautifulSoup(response, "lxml")
            except:
                continue

            jobList = soup.find_all("div", tag_main)
            for p in jobList:
                # check the data already exists or not 
                link = raw_link+p.find('a').get('href')

                # mengecek apakah data sudah ada atau belum
                cursor.execute("SELECT * FROM lowongan WHERE link_lowongan = '" + link + "'")
                data = cursor.fetchall()
                # if else untuk pengecekan input data
                if data:
                    exist += 1
                else:
                    add += 1

                    lowongan = p.find(tag_lowongan_part, tag_lowongan).get_text().replace(",", "").replace("'", "").replace('"', '')
                    # menggunakan try except karena ada beberapa perusahaan yang dirahasiakan
                    try:
                        perusahaan = p.find(tag_perusahaan_part, tag_perusahaan).get_text().replace(",", "").replace("'", "").replace('"', '')
                    except:
                        perusahaan = "Perusahaan Dirahasiakan"

                    try:
                        lokasi = p.find(tag_lokasi_part, tag_lokasi).get_text().replace("head office - ", "").replace(",", "").replace("'", "").replace('"', '')
                    except:
                        lokasi = "-"

                    # try print return scrapeDetail
                    keterangan, skill, benefit, deskripsi = Scrape.scrapeDetail(link, tag_mainDetail, tag_keterangan, tag_skill, tag_benefit, tag_deskripsi, breakDeskripsi)
                    
                    # stemming for detail
                    raw_stem = lowongan + " " + perusahaan + " " + lokasi + " " + keterangan + " " + skill + " " + benefit + " " + deskripsi
                    stem = Scrape.stemming(raw_stem)
                    
                    # save data to database
                    cursor.execute(
                        "INSERT INTO lowongan(asal_situs, title_lowongan, nama_perusahaan, lokasi_perusahaan, keterangan_lowongan, skill_lowongan, benefit_lowongan, deskripsi_lowongan, stem_detail, link_lowongan)"
                        "VALUES ('"+ asal +"', '"+ lowongan +"', '"+ perusahaan +"', '"+ lokasi +"', '"+ keterangan +"', '"+ skill +"', '"+ benefit +"', '"+ deskripsi  +"', '"+  stem +"', '"+ link +"')"
                        )
                    db.commit()

        # tampil data scrape
        print("Data yang ditambah: " + str(add))
        print("Data yang sudah ada: " + str(exist))

    def scrapeDetail(linkDetail, tag_mainDetail, tag_keterangan, tag_skill, tag_benefit, tag_deskripsi, breakDeskripsi):
        try:
            # hit the URL and fetch data
            r = Request(linkDetail, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'})
            gcontext = ssl.SSLContext()  # Only for gangstars
            response = urlopen(r, context=gcontext).read()
            soup = BeautifulSoup(response, "lxml")
        except Exception as e:
            # retry in case of an error
            print(str(e) + "- retry scrapeDetail() for: " + linkDetail)
            return Scrape.scrapeDetail(linkDetail, tag_mainDetail, tag_keterangan, tag_skill, tag_benefit, tag_deskripsi, breakDeskripsi); 

        raw_detail = soup.find_all("div", tag_mainDetail)
        for p in raw_detail:
            # menggunakan try except karena ada beberapa estimasi keterangan yang tidak terlampir
            try:
                raw_keterangan = p.find("div", tag_keterangan ).get_text(separator=". ").replace("Fungsi Kerja. ", "").replace(",", "").replace("'", "").replace('"', '')
                nltk_tokens = nltk.sent_tokenize(raw_keterangan)
                keterangan = ""

                for x in nltk_tokens:
                    if x == "Lamar.":
                        break
                    keterangan = keterangan + " " + x
            except:
                keterangan = "-"
            
            try:
                skill = p.find("div", tag_skill).get_text(separator=" ").replace(",", "").replace("'", "").replace('"', '')
            except:
                skill = '-'
            try:
                benefit = p.find("div", tag_benefit).get_text(separator=" ").replace("Tunjangan dan keuntungan", "keuntungan:").replace(",", "").replace("'", "").replace('"', '')
            except:
                benefit = '-'

            # menggunakan try except karena ada beberapa deskripsi yang NoneType
            try:
                # replace untuk glints
                raw_deskripsi = p.find("div", tag_deskripsi ).get_text(separator=". ").replace("Informasi Penting. Pastikan perusahaan yang kamu lamar resmi dengan memeriksa website dan lowongan kerja mereka.. Read Less.", "").replace(",", "").replace("'", "").replace('"', '').replace("\r\n", "").replace("\xa0", "")
                nltk_tokens = nltk.sent_tokenize(raw_deskripsi)
                deskripsi = ""

                for x in nltk_tokens:
                    if x == breakDeskripsi:
                        break
                    deskripsi = deskripsi + " " + x

                # remove punctuation from deskripsi
                deskripsi = Scrape.removePunctuation(deskripsi)
            except:
                deskripsi = "-"
            
            # give to scrapeMain
            return keterangan, skill, benefit, deskripsi

    def removePunctuation(InputString):
        # define punctuation
        punctuations = '''!()-[]{};:=+`'",<>./|\?@#$%^&*_~'''

        # remove punctuation from the string
        no_punct = ""
        for char in InputString:
            if char not in punctuations:
                no_punct = no_punct + char
            else:
                no_punct = no_punct + " "
        
        return no_punct

    def stemming(raw_stem):
        # call remove punctuation
        stem = Scrape.removePunctuation(raw_stem).lower()

        # create stemmer
        # nltk
        ps = PorterStemmer()
        # sastrawi
        factory = StemmerFactory()
        stemmer = factory.create_stemmer()

        # stemming process
        # nltk
        words = word_tokenize(stem)
        nltk_stemmer = ""
        for w in words:
            nltk_stemmer = nltk_stemmer + " " + ps.stem(w)
        # sastrawi sekaligus return
        return stemmer.stem(nltk_stemmer)

# # panggil classnya
# for x in range(2):
#     print(x) 
#     Scrape()

Scrape()