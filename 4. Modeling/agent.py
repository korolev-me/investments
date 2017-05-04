import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
from scipy.stats.mstats import gmean
import datetime

class Exc(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Agent:
    """
    Агент

    Attributes:
        envir:            Окружающая среда
        param_hist_df:    Параметры ПИФов история
        param_df:         Параметры ПИФов
        port_summ_value:  Суммарная стоимость портфеля без учёта наличных (приведённая)
        part_curr_df:     Текущие доли ПИФов в портфеле В ЦЕНАХ ПИФ
        part_req_hist_df: Требуемые доли ПИФов в портфеле история
        part_req_df:      Требуемые доли ПИФов в портфеле
        port_hist_df:     Портфель ПИФов история
        price_hist_df:    Цены ПИФов на дату
        port_data_df:     Данные портфеля ПИФов
        port_data_hist_df:Данные портфеля ПИФов история
        min_thres:        Минимальный порог прибыльности ПИФа
        top_thres:        Количественный порог прибыльности ПИФа (топ рейтинга по прибыльности)
        prim_state:       Состояние первичной закупки ПИФов
        den_pow:          Знаменатель степени 1/2**248/den_pow в которую возводится волатильность 

    """    

    def __init__(self, envir):
        self.envir = envir
        self.envir.agent = self

        self.start()

    def start(self, top_thres = 5 , min_thres = 1, pow_st_dev = 0.5, den_pow = 248):
        """
            Установка всех переменных (которые меняются при активности) в начальное состояние

        Args:

        """
        self.param_hist_df = pd.DataFrame (columns = ['date', 'pif_id', 'gm_w', 'total_1', 'st_dev_w', 'total_2'])
        self.part_req_hist_df = pd.DataFrame (columns = ['date', 'pif_id', 'part'])
        self.port_hist_df = pd.DataFrame (columns = ['date', 'pif_id', 'quantity'])
        self.price_hist_df = pd.DataFrame (columns = ['date', 'pif_id', 'price'])
        self.port_data_hist_df = pd.DataFrame (columns = ['date', 'pif_id', 'quantity', 'company', 'pif', 'min_sum', 'surcharge', 'discount', 'price', 'value', 'part'])
        
        self.min_thres = min_thres
        self.top_thres = top_thres
        self.pow_st_dev = pow_st_dev
        self.prim_state = True
        self.den_pow = den_pow

    def action(self):
        """
        Запуск функционирования агента циклом смены периодов
        """

        # Цикл функционирования агента и смены периодов
        self.new_period() 

        while self.envir.new_period():
            self.new_period()

        # Передача управления superviser для сохранения в историю
        self.suvis.action_finish()

    def new_period(self):
        """
        Отработка нового периода с расчётом параметров и изменением пропорций в портфеле
        """
        #print(self.envir.date, end = ' ')

        self.calc_param() #Запускать только 1 раз за 1 дату - иначе данные дублируются

        self.chang_port()

    def calc_param(self):
        """
        Расчёт показателей ПИФов: средняя доходность, волатильность и т.д.
        """

        # Цикл обхода ПИФов по справочнику
        for index in self.envir.spr_df.index:
            
            # Получение спарвочной информации по ПИФу с полями: pif_id, company, pif, min_sum, surcharge, discount
            spr_se = self.envir.spr_df.loc[index] 
            
            # Получение истории цен ПИФа отсортированной по дате
            price_hist_temp_df = self.envir.price_hist_df[self.envir.price_hist_df['pif_id'] == spr_se['pif_id']].sort_values(by = 'date', ascending=False).reset_index(drop=True)

            # Если история собрана менее чем за 2 года, то не рассматривать ПИФ
            if ((np.timedelta64(price_hist_temp_df['date'].max() - price_hist_temp_df['date'].min()).astype('timedelta64[Y]').item()) < 2) | (price_hist_temp_df.shape[0] < 496):
                continue

            # Замер длительности
            #time_curr = datetime.datetime.now()

            # Непосредственный рассчёт показателей
            price_hist_prev_df = price_hist_temp_df[248:]
            price_hist_prev_df.index = price_hist_temp_df.index[:-248]
            price_hist_temp_df = pd.merge(price_hist_temp_df, price_hist_prev_df[['date', 'price']], left_index = True, right_index=True, suffixes=('', '_prev'))
            #price_hist_temp_df['price_prev'] = price_hist_temp_df.apply(lambda x: (price_hist_temp_df['price'][x.name+248]) if x.name+248 <= price_hist_temp_df.index.max() else None, axis=1)
            #price_hist_temp_df['date_prev'] = price_hist_temp_df.apply(lambda x: (price_hist_temp_df['date'][x.name+248]) if x.name+248 <= price_hist_temp_df.index.max() else None, axis=1)

            # Замер длительности
            #time_prev = time_curr
            #time_curr = datetime.datetime.now()
            #print('Длительность : ', time_curr - time_prev)

            price_hist_temp_df = price_hist_temp_df[price_hist_temp_df['price_prev'].notnull() & (price_hist_temp_df.index <= 1240)]
            price_hist_temp_df['res'] = price_hist_temp_df['price']/price_hist_temp_df['price_prev']
            price_hist_temp_df['res'] = price_hist_temp_df['res'].astype(float)
            price_hist_temp_df['power'] = price_hist_temp_df.index/self.den_pow
            price_hist_temp_df['weight'] = 1/2**price_hist_temp_df['power']
            price_hist_temp_df['weight_rel'] = price_hist_temp_df['weight']/(price_hist_temp_df['weight'].sum())
            price_hist_temp_df['weight_res'] = price_hist_temp_df['weight_rel']*np.log(price_hist_temp_df['res'])
            price_hist_temp_df['dif'] = price_hist_temp_df['res']/gmean(price_hist_temp_df['res'])
            price_hist_temp_df['ln_dif'] = np.log(price_hist_temp_df['dif'])
            price_hist_temp_df['ln_dif_2'] = price_hist_temp_df['ln_dif']**2
            price_hist_temp_df['ln_dif_2_weight'] = price_hist_temp_df['weight_rel'] * price_hist_temp_df['ln_dif_2']
            
            del price_hist_temp_df['power']

            # Расчёт итоговых сводных показателей
            gm_w = np.exp(price_hist_temp_df['weight_res'].sum())
            total_1 = gm_w / (1 + spr_se['surcharge']) * (1 - spr_se['discount'])
            st_dev_w = np.exp((price_hist_temp_df['ln_dif_2_weight'].sum())**0.5)
            total_2 = total_1 / (st_dev_w ** self.pow_st_dev)
            
            # Сохранение в историю текущих параметров
            self.param_hist_df = self.param_hist_df.append({'date': self.envir.date, 'pif_id': spr_se['pif_id'], 'gm_w': gm_w, 'total_1': total_1, 'st_dev_w': st_dev_w, 'total_2': total_2}, ignore_index = True)


        # Получение текущих параметров обратно из истории
        self.param_df = self.param_hist_df[self.param_hist_df['date'] == self.envir.date]

        # Сохранение в историю текущего портфеля
        port_temp_df = self.envir.port_df.copy()
        port_temp_df['date'] = self.envir.date
        self.port_hist_df = self.port_hist_df.append(port_temp_df)

        # Сохранение в историю текущих цен
        self.price_hist_df = self.price_hist_df.append(self.envir.price_df)

        # Сохранение в историю полных данных портфеля с долями
        port_data_df = pd.merge (self.envir.port_df, self.envir.spr_df, on = ['pif_id'])
        port_data_df = pd.merge (port_data_df, self.envir.price_df, on = ['pif_id'])
        port_data_df['value'] = port_data_df['quantity'] * port_data_df['price'] * (1-port_data_df['discount'])
        port_data_df['part'] = (0 if port_data_df['value'].sum() == 0 else port_data_df['value']/port_data_df['value'].sum())
        #port_data_df['date'] = self.envir.date
        self.port_data_df = port_data_df

        self.port_data_hist_df = self.port_data_hist_df.append(port_data_df)

        # Подсчёт текущей суммарной стоимости портфеля в приведённых ценах
        self.port_summ_value = port_data_df['value'].sum()

        # Распечатка текущих показателей
        #print ('Текущие цены: \n', self.envir.price_df)
        #print ('\r', self.envir.date, 'port_summ_value: ', self.port_summ_value, end ='')
        return True

    def chang_port(self, cash = 1000000): # Данная величина cash устанволена и при первичном закупе у envir. Менять синхорнно
        """
        Расчёт и при необходимости изменение долей в инвестиционном портфеле
        """

        # Вычислить текущие доли паёв В ЦЕНАХ ПИФ
        part_curr_df = pd.merge (self.envir.port_df,  self.envir.spr_df, on = ['pif_id'])
        part_curr_df = pd.merge (part_curr_df, self.envir.price_df, on = ['pif_id'])
        part_curr_df['value'] = part_curr_df['quantity'] * part_curr_df['price'] * (1-part_curr_df['discount'])
        part_curr_df['part'] = (0 if part_curr_df['value'].sum() == 0 else part_curr_df['value']/part_curr_df['value'].sum())
        part_curr_df = part_curr_df[['pif_id', 'part']]
        # Текущие доли паёв В ЦЕНАХ ПИФ
        # Поля: pif_id, part
        self.part_curr_df = part_curr_df
        
        # Оставить ПИФы выше порогов
        part_req_df = self.param_df[self.param_df['total_2'] > self.min_thres]
        part_req_df = part_req_df.sort_values(by = ['total_2']).reset_index(drop = True).loc[0:self.top_thres]

        # Вычислить требуемые доли паёв
        part_req_df.loc[:,'bet_thres'] = part_req_df['total_2']-self.min_thres
        part_req_df['part'] = (0 if part_req_df['bet_thres'].sum() == 0 else part_req_df['bet_thres'] / part_req_df['bet_thres'].sum()) #part_req_df['bet_thres'] / part_req_df['bet_thres'].sum()
        part_req_df = part_req_df[['pif_id', 'part']]        
        # Требуемые доли паёв В ЦЕНАХ ПИФ
        # Поля: pif_id, part
        self.part_req_df = part_req_df

        # Сохранить в историю требуемые доли паёв В ЦЕНАХ ПИФ
        part_req_temp_df = part_req_df
        part_req_temp_df['date'] = self.envir.date
        # История требуемых долей паёв В ЦЕНАХ ПИФ
        # Поля: date, pif_id, part
        self.part_req_hist_df = self.part_req_hist_df.append(part_req_temp_df, ignore_index = True)
    
        # Вычислить доли паёв, которые необходимо купить/продать
        port_chang_df = pd.merge(
            pd.DataFrame(part_curr_df [['pif_id', 'part']].values, columns = ['pif_id', 'part_curr']),
            pd.DataFrame(part_req_df [['pif_id', 'part']].values, columns = ['pif_id', 'part_req']),
            how = 'outer', on = 'pif_id').fillna(value=0)
        
        if (port_chang_df.shape[0] == 0):
            port_chang_df['part_sell'] = None
            port_chang_df['part_buy'] = None
        else:
            port_chang_df['part_sell'] = port_chang_df.apply(lambda row: (row['part_curr'] - row['part_req']) if (row['part_curr'] > row['part_req'])  else 0, axis=1)
            port_chang_df['part_buy'] = port_chang_df.apply(lambda row: (row['part_req'] - row['part_curr']) if (row['part_curr'] < row['part_req'])  else 0, axis=1)
        # Доли паёв, которые необходимо купить/продать
        # Поля: pif_id, part_curr, part_req, part_sell, part_buy
        self.port_chang_df = port_chang_df

        if self.prim_state:
            cash_temp = cash
            for index in part_req_df.index:
                row_buy = part_req_df.loc[index]
                part_temp = row_buy['part']

                # Проверка на соответствие требованиям минимальной стоимости приобретения ПИФа
                if (part_temp * cash_temp > self.envir.spr_df[self.envir.spr_df['pif_id'] == row_buy['pif_id']].iloc[0]['min_sum']):

                    # Покупка ПИФа
                    self.buy(row_buy['pif_id'], part_temp * cash_temp) #!!! применить обратно наценку как выше при продаже - переделать на приведённую стоимость
                    self.prim_state = False
        else:
            # Рассчитать матрицу выгодности перекупки паёв
            # Выгода от смены паёв
            self.benef_shar = pd.DataFrame (columns = ['pif_id_sell', 'pif_id_buy', 'benef'])
            port_ch_param_df = pd.merge(port_chang_df, self.param_df[['pif_id', 'total_2']], on = 'pif_id')
            port_ch_param_df = pd.merge(port_ch_param_df, self.envir.spr_df[['pif_id', 'surcharge', 'discount']], on = 'pif_id')
            # Доли паёв, которые необходимо купить/продать с параметрами
            # Поля: pif_id, part_curr, part_req, part_sell, part_buy, gm_w
            self.port_ch_param_df = port_ch_param_df

            # Распечатка всех показателей
            #print ('Смена портфеля: \n', port_chang_df)
            #print ('Параметры ПИФов: \n', port_ch_param_df)

            # Цикл расчёта выгодности перехода по необходимым ПИФам
            for index_sell in port_ch_param_df[port_ch_param_df['part_req'] < port_ch_param_df['part_curr']].index: # После отладки убрать объединение со справочником 
                pif_sell = port_ch_param_df.loc[index_sell]
                for index_buy in port_ch_param_df[port_ch_param_df['part_req'] > port_ch_param_df['part_curr']].index:
                    pif_buy = port_ch_param_df.loc[index_buy]
                    benef = pif_buy['total_2'] - (pif_sell['total_2'] * (1 + pif_sell['surcharge']) / (1 - pif_sell['discount'])) # Т.к. в total_2 скидка и надбавка уже учтены, то применяем их обратно

                    #print ('pif_sell: ', pif_sell['pif_id'], 'pif_buy: ', pif_buy['pif_id'], 'benef: ', benef)        
                    self.benef_shar = self.benef_shar.append({'pif_id_sell': pif_sell['pif_id'], 'pif_id_buy': pif_buy['pif_id'], 'benef': benef}, ignore_index = True)
            self.benef_shar = self.benef_shar[self.benef_shar['benef'] > 0].sort_values(by = 'benef', ascending=False)

            # Продать/купить доли паёв
            # Таблица у которой будут учитываться (убавляться) доли для смены
            temp_chang_df = port_chang_df[['pif_id', 'part_sell', 'part_buy']]
            self.temp_chang_df = temp_chang_df
            for index in self.benef_shar.index:
                #print (self.benef_shar.loc[index])
                row_sell = temp_chang_df[temp_chang_df['pif_id'] == self.benef_shar.loc[index]['pif_id_sell']].iloc[0]
                row_buy = temp_chang_df[temp_chang_df['pif_id'] == self.benef_shar.loc[index]['pif_id_buy']].iloc[0]
                #print('part_sell: ', row_sell['part_sell'], 'index: ', row_sell.name)
                #print('part_buy: ',  row_buy['part_buy'], 'index: ', row_buy.name)

                # Выбираем меньшую долю из необходимых покупки и продажи
                if row_sell['part_sell'] > row_buy['part_buy']:
                    #print ('buy smaller')
                    part_temp = row_buy['part_buy']
                else:
                    #print ('sell smaller')
                    part_temp = row_sell['part_sell']                

                # Проверка на соответствие требованиям минимальной стоимости приобретения ПИФа
                if (part_temp * self.port_summ_value > self.envir.spr_df[self.envir.spr_df['pif_id'] == row_buy['pif_id']].iloc[0]['min_sum']):

                    # Продажа ПИФа
                    self.sell(row_sell['pif_id'], part_temp * self.port_summ_value)

                    #temp_chang_df.loc[row_sell.name]['part_sell'] = row_sell['part_sell'] - part_temp
                    temp_chang_df.set_value(row_sell.name, 'part_sell', row_sell['part_sell'] - part_temp)

                    #исправление ошибки float                
                    if temp_chang_df.loc[row_sell.name]['part_sell'] < 1e-9: 
                        temp_chang_df.loc[row_sell.name]['part_sell'] = 0

                    # Покупка ПИФа
                    self.buy(row_buy['pif_id'], part_temp * self.port_summ_value)
                    temp_chang_df.loc[row_buy.name]['part_buy'] = row_buy['part_buy'] - part_temp
                    #исправление ошибки float                
                    if temp_chang_df.loc[row_sell.name]['part_buy'] < 1e-9: 
                        temp_chang_df.loc[row_sell.name]['part_buy'] = 0

    def buy(self, pif_id, value):

        #исправление ошибки float
        if ((value - self.envir.cash) < 1e-9) & ((value - self.envir.cash) > -1e-9):
            value = self.envir.cash

        self.envir.buy(pif_id, value)
        return True
    
    def sell(self, pif_id, value):

        # Получение справочной информации по ПИФу
        spr_pif_temp_df = self.envir.spr_df[self.envir.spr_df['pif_id'] == pif_id]
        if (spr_pif_temp_df.shape[0] == 0):
            raise Exc('There is no such pif ' + str(pif_id) + ' in spr')
        spr_pif = spr_pif_temp_df.iloc[0]
        
        # Получение текущей цены по ПИФу
        price_temp_df = self.envir.price_df[(self.envir.price_df['pif_id'] == pif_id)]
        if (price_temp_df.shape[0] == 0):
            raise Exc('There is no such pif ' + str(pif_id) + ' in price')
        price_se = price_temp_df.iloc[0]

        # Получение портфельной записи по ПИФу
        port_temp_df = self.envir.port_df[self.envir.port_df['pif_id'] == pif_id]
        if (port_temp_df.shape[0] == 0):
            raise Exc('There is no such pif ' + str(pif_id) + ' in port')
        port_se = port_temp_df.iloc[0]

        #Вычисление количетсва ПИФов на продажу
        pif_quantity = value / (price_se['price'] * (1 - spr_pif.discount))

        #исправление ошибки float
        #if ((value - (port_se['quantity'] * price_se['price'] * (1 - spr_pif.discount))) < 1e-9) & ((value - (port_se['quantity'] * price_se['price'] * (1 - spr_pif.discount))) > -1e-9):
        #    value = port_se['quantity'] * price_se['price'] * (1 - spr_pif.discount)
        if (port_se['quantity'] - pif_quantity < 1e-9) & (port_se['quantity'] - pif_quantity > -1e-9):
            value = port_se['quantity'] * (price_se['price'] * (1 - spr_pif.discount))

        self.envir.sell(pif_id, value)
        return True
