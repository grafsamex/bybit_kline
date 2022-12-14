from pybit import spot
import datetime
import sqlite3 as sl


def bybit_db_pair():
    connect_db = sl.connect(r'../criptobase.db')
    cursor = connect_db.cursor()
    cursor.execute("SELECT name FROM bybit_currency")
    bybit_pair = cursor.fetchall()
    bybit_pair = [i for sub_list in bybit_pair for i in sub_list]
    cursor.close()
    connect_db.close()
    return bybit_pair

def get_timing():
    while True:
        time = input('Введите временной интервал:\n'
          '1 - 1 минута\n'
          '3 - 3 минуты\n'
          '5 - 5 минут\n'
          '15 - 15 минут\n'
          '60 - 1 час\n')
        if time not in ['1', '3', '5', '15', '60']:
            print('Введено некорректное значение временного интервала!!!\n'
              'Повторите ввод данных!!!')
            continue
        break
    return time


def get_bybit_kline(pair, timing):
    while True:
        try:
            session_unauth = spot.HTTP(endpoint="https://api.bybit.com")
            print('Серверное время BYBIT: ',
                  datetime.datetime.fromtimestamp(float(session_unauth.server_time()['time_now'])))
            print(f"Всего торговых пар для получения данных: {len(pair)}")
            break
        except:
            print('Подключение не удалось выполнить. Повторить попытку?')
            connect_bybit = input('Для повторной попытки нажмите любую клавишу. Для прерывания операции введите "NO": ')
            print(connect_bybit)
            if connect_bybit.lower() == 'no' or connect_bybit.lower() == 'тщ':
                return
            else:
                continue
    connect_db = sl.connect(r'../criptobase.db')
    cursor = connect_db.cursor()
    time_step = timing + 'm'

    for i in range(len(pair)):
        bybit_kline = session_unauth.query_kline(symbol=pair[i], interval=time_step)['result']
        cursor.execute(f"CREATE TABLE IF NOT EXISTS bybit_kline_{pair[i]+'_'+time_step} (starttime INTEGER,\
                       open_price REAL, high_price REAL, low_price REAL, close_price REAL,\
                       end_time REAL, qa_volume REAL, num_trader INTEGER) ;")
        connect_db.commit()
        kline = []
        for j in range(len(bybit_kline)):
            kline.append((bybit_kline[j][0], float(bybit_kline[j][1]), float(bybit_kline[j][2]),
                          float(bybit_kline[j][3]), float(bybit_kline[j][4]), float(bybit_kline[j][5]),
                          float(bybit_kline[j][7]), int(bybit_kline[j][8])))
        cursor.execute(f"SELECT starttime FROM bybit_kline_{pair[i]+'_'+time_step};")
        db_kline_tuple = cursor.fetchall()
        db_kline_list = [db_kline_tuple[i][0] for i in range(len(db_kline_tuple))]
        kline_db = []
        for k in range(len(kline)):
            if kline[k][0] not in db_kline_list:
                kline_db.append(kline[k])
        cursor.executemany(f"INSERT INTO bybit_kline_{pair[i] + '_' + time_step} VALUES(?, ?, ?, ?, ?, ?, ?, ?);", kline_db)
        connect_db.commit()
    cursor.close()
    connect_db.close()





if __name__ == '__main__':
    print('Загрузка торговых данных (свечей) с Bybit\n'
          'Выберите необходимое действие:\n'
          '1. Загрузить данные по одной паре\n'
          '2. Загрузить данные по всем парам\n'
          '3. Отменить загрузку')
    menu_action = input()
    if menu_action == '1':
        while True:
            pair2 = (input('Введите название торговой пары:\n')).upper()
            if pair2 not in bybit_db_pair():
                print('!!!Значение отсутствует!!!')
                if input('Ввести новое значение? 1-Да/2-Нет') == '1':
                    continue
                else:
                    break
            timing = get_timing()
            pair = list()
            pair.append(pair2)
            get_bybit_kline(pair, timing)
            break
    elif menu_action == '2':
        pair = bybit_db_pair()
        timing = get_timing()
        get_bybit_kline(pair, timing)
    elif menu_action == '3':
        print('Загрузка отменена')
    else:
        print('Введена некорректная команда')
    print('Загрузка завершена')

