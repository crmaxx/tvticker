#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import string
"""Для md5"""
import hashlib
"""Для запроса XML с сайта"""
import urllib
"""Для пидов"""
import fcntl
"""Для перекодировки"""
import locale
"""Для процессов"""
#import thread
"""Для таймера"""
import time

# TODO: Добавить нормальное логирование через import logging
# TODO: Добавить нормальный конфиг через import ConfigParser

# Для разбора XML
from xml.dom.minidom import parseString

islock = True
lock_path = '/home/tvchat/bin/tvchat.lock'
enc = locale.getpreferredencoding() 

####################################################
### Переменные для настройки #######################
####################################################

""" Шаблон сообщений: 
0 - имя файла, 
1 - расширение
"""
tmpl = ['messages', 'sms']
"""Потоки/директории"""
stream = ['/home/tvchat/sms/']
"""Количество файлов"""
file_cnt = 3
"""Логин и пароль от сервиса"""
LGN = 'admin'
PWD = 'admin'
"""Число отгружаемых сообщений"""
CNT = 30
""" Дата, 
начиная с которой выбираются сообщения 
в формате YYYY-MM-DD"T"HH24:MI:SS
"""
DATA = '2009-01-01T00:00:00'
""" Порядок вывода сообщений:
0 - выбрать первые "c" сообщений, 
1 - выбрать последние "с" сообщений).
"""
P = 1

####################################################

def obf(login, password):
    """Функция для обфускации логина и пароля"""
    m = hashlib.md5()
    m.update(login)
    m.update(password)
    return m.hexdigest()

def get_sms(xfile):
    """Получаем xml файл с сайта"""
    OBF = obf(LGN, PWD)
    params = urllib.urlencode({'h': OBF, 'c': CNT, 'd': DATA, 'p':P})
    xml = urllib.urlopen("http://127.0.0.1/pls/apex/cms.txml?%s" % params)
    #link = 'https//127.0.0.1/pls/apex/cms.txml?h='+OBF+'&c='+str(CNT)+'&d='+DATA+'&p='+str(P)
    #print link
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
    #print files, datas
    for f, d in map(None, files, datas):
        if d != None:
            open(f, 'wb').write(d.encode(enc, "replace"))

def make_filename(bgn, end):
    """Сгенерировать очередные имена файлов"""
    result = []
    # FIXME: !!! Правильную генерацию сделать. Генерит лишние списки !!!
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
            #print sms_list_cnt, strm, tmp
            while sms_list_tmp >= 0:
                #print curr, strm, sms_list_tmp, file_cnt, tmp
                # !
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
                    sms_list_tmp = sms_list_tmp - file_cnt
                    curr = curr + tmp
                    tmp = tmp + tmp

                #print new_sms, files, file_list
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
        except Exception,msg:
            """ Если скрипт предусматривает режим ожидание, то сообщение
            пишется на stdout при условии "if isverbose:", иначе
            администартор будет засыпан письмами о повторном запуске и
            не сможет работать.
            """

            #print "Повторный запуск процесса"
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
