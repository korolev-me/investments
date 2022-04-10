import pandas as pd
import yfinance as yf
from pandas.tseries.offsets import *
import os

class Exc(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Environment:
    """
    Окружающая среда

    Attributes:

        type_instr:     Тип инсрумента инвестиций
        spr_df:         Справочник инструментов
        _spr_df:        Справочник инструментов полный
        price_df:       Цены инструментов
        price_hist_df:  Цены инструментов история видимая для агента на определённую дату
        _price_hist_df: Цены инструментов история полная
        port_df:        Портфель инструментов
        cash:           Денежные средства
        cash_start:     Денежные средства стартовые
        date_min:       Дата минимальная
        date_max:       Дата максимальная
        date:           Дата
        date_start:     Дата стартовая
        date_finish:    Дата стартовая
    """
    
    
    def __init__(self, date_start, cash_start, type_instr, file_extension): # Данная величина cash устанволена и при первичном закупе у agent. Менять синхорнно
        
        self.type_instr = type_instr

        #self._spr_df = pd.read_excel(os.path.join('3. Data preparation', self.type_instr, 'spr.xlsx'))
        # self._spr_df = pd.read_parquet(os.path.join('3. Data preparation', self.type_instr, 'spr.parquet'))
        self._spr_df = pd.read_csv(os.path.join('3. Data preparation', self.type_instr, 'spr.csv'))
        self._price_hist_df = pd.DataFrame (columns = ['date', 'instr_id', 'price', 'volume'])
        
        # Загрузка полной истории цен ниструментов
        
        for index, row in self._spr_df.iterrows():
            # filename = self._spr_df.loc[index]['manager'] + ' - ' + self._spr_df.loc[index]['instr'] + '.' + file_extension

            # hist_instr_temp_df = pd.read_excel(os.path.join('3. Data preparation', self.type_instr, filename), sheet_name = 0, header = 2)

            hist_instr_temp_df = yf.download(row['instr']) #, start='1990-01-01', end='2022-03-01'
            hist_instr_temp_df = hist_instr_temp_df.reset_index()
            hist_instr_temp_df = hist_instr_temp_df.rename(columns = {'Date': 'date', 'Open': 'open', 'High':'high', 'Low':'low', 'Close':'close', 'Adj Close': 'adj_close', 'Volume': 'volume'})

            hist_instr_temp_df['instr_id'] = row['instr']
            #self._price_hist_df = self._price_hist_df.append(hist_instr_temp_df, ignore_index=True)
            self._price_hist_df = pd.concat([self._price_hist_df, hist_instr_temp_df])
        # self._price_hist_df['instr_id'] = self._price_hist_df['instr_id'].astype(int)
        self._price_hist_df['date'] = pd.to_datetime(self._price_hist_df['date'], dayfirst = True)
        self._price_hist_df['price'] = self._price_hist_df['close']

        #Установка дат
        self.date_min = min(self._price_hist_df['date'])# + MonthBegin(n=0)
        self.date_max = max(self._price_hist_df['date'])# + MonthBegin(n=1) - MonthBegin(n=1)

        self.start(date_start = date_start, cash_start = cash_start)

        return

    
    def start(self, date_start = pd.to_datetime('2010-01-01'), date_finish = pd.to_datetime('2020-01-01'), cash_start = 1_000_000):
        """
            Установка всех переменных (которые меняются при активности) в начальное состояние

        Args:
            cash_start:     Денежные средства стартовые
            date_start:     Дата стартовая

        """

        date = date_start# + MonthBegin(n=0)

        if (date_start > self.date_max):
            raise Exc('Date_start ' + str(date_start) + ' exceeding the maximum date '+ str(self.date_max))
        if (date_start < self.date_min):
            raise Exc('Date_start ' + str(date_start) + ' lowering the minimum date' + str(self.date_min))

        if (date_finish > self.date_max):
            raise Exc('Date_finish ' + str(date_finish) + ' exceeding the maximum date '+ str(self.date_max))
        if (date_finish < self.date_min):
            raise Exc('Date_finish ' + str(date_finish) + ' lowering the minimum date' + str(self.date_min))

        if (date > self.date_max):
            raise Exc('Date ' + str(date) + ' exceeding the maximum date '+ str(self.date_max))
        if (date < self.date_min):
            raise Exc('Date ' + str(date) + ' lowering the minimum date' + str(self.date_min))

        self.date_start = date_start
        self.date_finish = date_finish
        self.cash_start = cash_start
        self.cash = self.cash_start
        self.date = date

        self.port_df = pd.DataFrame (columns = ['instr_id', 'quantity'])
        
        self.calc_data()

        
    def new_period(self):

        """
            Переводит среду в состояние нового периода.
            Запускает рассчёт данных дл нового периода.

        Args:

        Returns:
            True: Если всё отработало правильно
            False: Если требуемая дата превышает максимальную
        Raises:

        """
        if (self.date + MonthBegin(n=1) > self.date_finish):
            return False
        else:
            self.date = self.date + MonthBegin(n=1)
            self.calc_data()
            # print('    new_period:', self.date)
            return True
    
    
    def calc_data(self):
        """
            Рассчёт данных для периода

        Args:

        Returns:
            True: Если всё отработало правильно

        Raises:
        """

        self.price_hist_df = self._price_hist_df[(self._price_hist_df['date'] <= self.date)]
        self.price_df = pd.merge(self.price_hist_df.groupby(['instr_id'], as_index = False)['date'].max(), self._price_hist_df, on = ['instr_id', 'date'])[['instr_id', 'price']]
        self.spr_df = self._spr_df[self._spr_df['instr_id'].isin(self.price_hist_df['instr_id'].unique()) == True]
        
        return True
    
    
    def buy(self, instr_id, value):
        """
            Осуществление покупки инструмента в портфель.

        Args:
            instr_id: 
                Id инструмента к покупке.
            value: 
                Приведёная стоимость, на которую необходимо приобрести инструмент.
                Стоимость указывается в деньгах до покупки - на сколько реально уменьшится
                количество денег при покупке.

        Returns:
            True: Если всё отработало правильно

        Raises:
            IOError: 
                There is no such instr _ in spr
                Value _ is less than the minimum sum _
                There is no such instr _ in price
                Value _ is more than balance of cash _
        """

        # Получение справочной информации по инструменту
        spr_instr_temp_df = self.spr_df[self.spr_df['instr_id'] == instr_id]
        if (spr_instr_temp_df.shape[0] == 0):
            raise Exc('There is no such instr ' + str(instr_id) + ' in spr')
        spr_instr = spr_instr_temp_df.iloc[0]
        
        # Порверка, что сумма покупки не меньше минимальной в приведёной стоимости
        if (value < spr_instr.min_sum):
            raise Exc('Value ' + str(value) + ' is less than the minimum sum ' + str(spr_instr.min_sum))

        # Получение текущей цены по инструменту
        price_temp_df = self.price_df[(self.price_df['instr_id'] == instr_id)]
        if (price_temp_df.shape[0] == 0):
            raise Exc('There is no such instr ' + str(instr_id) + ' in price')
        price_se = price_temp_df.iloc[0]
        
        # Получение портфельной записи по инструменту
        port_temp_df = self.port_df[self.port_df['instr_id'] == instr_id]
        if (port_temp_df.shape[0] == 0):
            self.port_df = pd.concat([self.port_df, pd.DataFrame.from_dict([{'instr_id': instr_id, 'quantity': 0}])])
            port_temp_df = self.port_df[self.port_df['instr_id'] == instr_id]
            
        port_se = port_temp_df.iloc[0]

        # Вычисление количетсва инструментов на приобретение
        instr_quantity = value / (price_se['price'] * (1 + spr_instr.surcharge))

        # Проверка остатка на балансе кэша
        if (value - self.cash  > 1e-9):
            raise Exc('Value ' + str(value) + ' is more than balance of cash ' + str(self.cash) + '. Difference: ' + str(value - self.cash))
            
        # Покупка
        self.cash = self.cash - value
        self.port_df.loc[port_se.name, 'quantity'] = port_se['quantity'] + instr_quantity

        return True
    
    
    def sell(self, instr_id, value):
        """
            Осуществление продажи инструмента.

        Args:
            instr_id: 
                Id инструмента к продаже.
            value: 
                Приведёная стоимость, на которую необходимо продать инструмент.
                Стоимость указывается в деньгах после продажи - на сколько реально увеличится
                количество денег при продаже.

        Returns:
            True: Если всё отработало правильно

        Raises:
            IOError: 
                There is no such instr _ in spr
                There is no such instr _ in price
                There is no such instr _ in port
                Value _ is more than balance of instr _
        """

        # Получение справочной информации по инструменту
        spr_instr_temp_df = self.spr_df[self.spr_df['instr_id'] == instr_id]
        if (spr_instr_temp_df.shape[0] == 0):
            raise Exc('There is no such instr ' + str(instr_id) + ' in spr')
        spr_instr = spr_instr_temp_df.iloc[0]
        
        # Получение текущей цены по инструменту
        price_temp_df = self.price_df[(self.price_df['instr_id'] == instr_id)]
        if (price_temp_df.shape[0] == 0):
            raise Exc('There is no such instr ' + str(instr_id) + ' in price')
        price_se = price_temp_df.iloc[0]

        # Получение портфельной записи по инструменту
        port_temp_df = self.port_df[self.port_df['instr_id'] == instr_id]
        if (port_temp_df.shape[0] == 0):
            raise Exc('There is no such instr ' + str(instr_id) + ' in port')
        port_se = port_temp_df.iloc[0]
            
        #Вычисление количетсва инструментов на продажу
        instr_quantity = value / (price_se['price'] * (1 - spr_instr.discount))
            
        # Проверка остатка на балансе инструмента
        #if (value > (port_se['quantity'] * price_se['price'] * (1 - spr_instr.discount))):
        #    raise Exc('Value ' + str(value) + ' is more than balance of instr ' + str(port_se['quantity'] * (price_se['price'] * (1 - spr_instr.discount))) + '. Difference: ' + str(value - (port_se['quantity'] * (price_se['price'] * (1 - spr_instr.discount)))))
        if (instr_quantity - port_se['quantity'] > 1e-9):
            raise Exc('Value ' + str(value) + ' is more than balance of instr ' + str(port_se['quantity'] * (price_se['price'] * (1 - spr_instr.discount))) + '. Difference: ' + str(value - (port_se['quantity'] * (price_se['price'] * (1 - spr_instr.discount)))))

        # Продажа
        self.port_df.set_value(port_se.name, 'quantity',  port_se['quantity'] - instr_quantity)
        self.cash = self.cash + value

        return True