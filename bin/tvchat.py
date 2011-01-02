#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import string
import hashlib # Для md5
import urllib  # Для запроса XML с сайта
import fcntl   # Для пидов
import locale  # Для перекодировки
import time    # Для таймера
import ConfigParser
import logging
#import thread # Для процессов

# Для разбора XML
from xml.dom.minidom import parseString

islock = True
lock_path = '/home/tvchat/bin/tvchat.lock'
enc = locale.getpreferredencoding()

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

"""Инициализируем модуль для работы с конфигами"""
config = ConfigParser.ConfigParser()
"""Подгружаем конфиг"""
config.read('tvchat.cfg')

"""Получаем основные переменные"""
tmpl = config.get("Default", "tmpl")
stream = config.get("Default", "stream")
file_count = config.get("Default", "file_count")
user = config.get("Default", "user")
password = config.get("Default", "password")
cnt = config.get("Default", "cnt")
data = config.get("Default", "data")
p = config.get("Default", "p")

"""Инициализируем систему логирования"""
level_name = config.get("Logs", "level")
level = LEVELS.get(level_name, logging.NOTSET)
logging.basicConfig(level = level,
		    format = config.get("Logs", "format", 1),
		    filename = config.get("Logs", "filename"),
		    filemode = 'w')

"""Создаём логгеры"""
main_logger = logging.getLogger('Main')

main_logger.info("Starting tvchat at %s", data())

####################################################

def obf(login, password):
    """Функция для обфускации логина и пароля"""
    m = hashlib.md5()
    m.update(login)
    m.update(password)
    return m.hexdigest()

def get_sms(xfile):
    """Получаем xml файл с сайта"""
    crpt = obf(user, password)
    params = urllib.urlencode({'h': crpt, 'c': cnt, 'd': data, 'p': p})
    xml = urllib.urlopen("http://127.0.0.1/pls/apex/cms.txml?%s" % params)
    #link = 'https//127.0.0.1/pls/apex/cms.txml?h='+OBF+'&c='+str(CNT)+'&d='+DATA+'&p='+str(P)
    #main_logger.debug("Working URI: %s", link)
    #xml = urllib.urlopen(link)
    tmp_file = open(xfile, 'wb')
    tmp_file.write(xml.read())
    tmp_file.close()
    xml.close()
    return 0

def parse_sms(xfile):
    """Функция преобразования xml в список"""
    result = []
    """открываем и читаем xml"""
    filename = open(xfile, "r")
    cont = filename.read()
    idx = string.index (cont, 'rs:message')
    if idx:
        """парсим"""
        dom = parseString(cont)
        """получаем сообщения в массив"""
        for res in dom.getElementsByTagName("rs:message"):
            result.append(res.getAttribute("id")
                      + ": "
                      + res.lastChild.data
                      + res.getAttribute("status")
                      + res.getAttribute("create-date"))
    return result

def get_new_sms(bgn, end,arr):
    """Взять N sms из массива, где N - количество потоков"""
    result = []
    result = arr[bgn:end]
    return result

def write_sms(files, datas):
    """Записать в каждый каталог по одному файлу с смс"""
    main_logger.debug("Current files = %s, data = %s", files, data)
    for f, d in map(None, files, datas):
        if d is not None:
            open(f, 'wb').write(d.encode(enc, "replace"))

def make_filename(bgn, end):
    """Сгенерировать очередные имена файлов"""
    result = []
    # FIXME: Правильную генерацию сделать. Генерит лишние списки.
    for i in xrange(bgn, end):
        u = tmpl[0] + str(i) + "." + tmpl[1]
        u.encode(enc, "replace")
        result.append(u)
    return result

def make_list(dirs, files):
    """Сгенерировать список файлов"""
    result = []
    if len(dirs) == len(files):
        for d, f in map(None, dirs, files):
            result.append(d+f)
    else:
        d = dirs[0]
        for f in files:
            result.append(d+f)
    return result

def fill_sms():
    """логика обработки списка"""
    while 1:
        curr = 0
        """Получаем отмодерированные смс"""
        get_sms('tmp.xml')
        """Преобразуем их в список"""
        sms_list = parse_sms('tmp.xml')
        """Удаляем временный файл."""
        os.remove('tmp.xml')
        """Инициализируем рабочие переменные"""
        sms_list_cnt = len(sms_list)
        strm = len(stream)
        sms_list_tmp = sms_list_cnt
        tmp = file_cnt

        if sms_list_cnt:
            main_logger.debug("Current sms_list_cnt = %s, strm = %s, tmp = %s)", sms_list_cnt, strm, tmp)
            while sms_list_tmp >= 0:
                main_logger.debug("Current curr = %s, strm = %s, sms_list_tmp = %s, file_cnt = %s, tmp = %s)",
                                  curr, strm, sms_list_tmp, file_cnt, tmp)
                if (file_cnt > 1) and (strm == 1):
                    """Берём пачку Новых СМС из списка"""
                    new_sms = get_new_sms(curr, tmp, sms_list)
                    """Генерируем список файлов"""
                    files = make_filename(curr, curr+file_cnt)
                    """Создать из пар директории / файлы общий список"""
                    file_list = make_list(stream, files)
                    """Пишем пачку файлов"""
                    write_sms(file_list, new_sms)

                    """Берём новую пачку смс"""
                    sms_list_tmp -= file_cnt
                    curr += tmp
                    tmp += tmp

                main_logger.debug("Current new_sms = %s, files = %s, file_list = %s", new_sms, files, file_list)
        time.sleep(10) # делаем паузу 10 секунд

# В конце скрипта
def main():
    """Проверяем наличие лока и ставим его, если лока нет"""
    if islock:
        try:
            _lock_file=open(lock_path, 'r+')
        except:
            _lock_file=open(lock_path, 'w+')
        try:
            fcntl.flock(_lock_file.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
        except Exception, msg:
            """ Если скрипт предусматривает режим ожидание, то сообщение
            пишется на stdout при условии "if isverbose:", иначе
            администартор будет засыпан письмами о повторном запуске и
            не сможет работать.
            """

            main_logger.debug("Recorded attempt to run the process.")
            sys.exit(2)

        _lock_file.write(str(os.getpid()))
        _lock_file.flush()

        """Обратите внимание, что файл должен оставаться открытым до
        завершения работы скрипта, поэтому, если локирование делается
        внутри метода, то _lock_file должен быть переменной класса или
        глобальной переменной, по обстоятельствам.
        """


    """если tmpl не пустой"""
    if tmpl:
        """запускаем функцию fill_sms в отдельном потоке"""
        #thread.start_new_thread(fill_sms,())
        fill_sms()

if __name__ == '__main__':
    main()
