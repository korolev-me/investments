import pandas as pd
from pandas.tseries.offsets import *

class Exc(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Environment:
    """
    Окружающая среда

    Attributes:

        type_insrt:    Тип инсрумента инвестиций
        spr_df:         Справочник ПИФов
        __spr_df:       Справочник ПИФов полный
        price_df:       Цены ПИФов
        price_hist_df:  Цены ПИФов история
        __price_hist_df:Цены ПИФов история полная
        port_df:        Портфель ПИФов
        cash:           Денежные средства
        cash_start:     Денежные средства стартовые
        date_min:       Дата минимальная
        date_max:       Дата максимальная
        date:           Дата
        date_start:     Дата стартовая
        date_finish:     Дата стартовая

    """
    
    def __init__(self, date_start, cash_start, type_insrt): # Данная величина cash устанволена и при первичном закупе у agent. Менять синхорнно
        
        self.type_insrt = type_insrt

        self.__spr_df = pd.read_excel('..\\3. Data preparation\\' + self.type_insrt + '\\spr.xlsx')
        self.__price_hist_df = pd.DataFrame (columns = ['date', 'insrt_id', 'price', 'asset_value'])
        
        # Загрузка полной истории цен ПИФов
        for index in self.__spr_df.index:
            filename = self.__spr_df.loc[index]['manager'] + ' - ' + self.__spr_df.loc[index]['insrt'] + '.xlsx'
            


            if (self.type_insrt == 'pif'):
                hist_insrt_temp_df = pd.read_excel('..\\3. Data preparation\\' + self.type_insrt + '\\' + filename, sheetname = 0, header = 2)
                hist_insrt_temp_df.columns = ['date', 'price', 'asset_value']

            elif (self.type_insrt == 'pamm'):
                hist_insrt_temp_df = pd.read_excel('..\\3. Data preparation\\' + self.type_insrt + '\\' + filename, sheetname = 0, header = 4)
                hist_insrt_temp_df.reset_index(inplace = True)
                hist_insrt_temp_df.columns = ['date', 'rev_open', 'rev_high', 'rev_low', 'rev_close', 'torg_open', 'torg_high', 'torg_low', 'torg_close', 'asset_value']
                hist_insrt_temp_df['price'] = (hist_insrt_temp_df['rev_close']/100)+1
                hist_insrt_temp_df = hist_insrt_temp_df[['date', 'price', 'asset_value']]

            hist_insrt_temp_df.columns = ['date', 'price', 'asset_value']

            hist_insrt_temp_df['insrt_id'] = self.__spr_df.loc[index]['insrt_id']
            self.__price_hist_df = self.__price_hist_df.append(hist_insrt_temp_df, ignore_index=True)
        self.__price_hist_df['insrt_id'] = self.__price_hist_df['insrt_id'].astype(int)
        self.__price_hist_df['date'] = pd.to_datetime(self.__price_hist_df['date'], dayfirst = True)
        # Исключение из справочника 'asset_value'
        self.__price_hist_df = self.__price_hist_df[['date', 'insrt_id', 'price']]

        #Установка дат
        self.date_min = min(self.__price_hist_df['date']) + MonthBegin(n=0)
        self.date_max = max(self.__price_hist_df['date']) + MonthBegin(n=1) - MonthBegin(n=1)

        self.start(date_start = date_start, cash_start = cash_start)

        return

    def start(self, date_start = pd.to_datetime('2014-01-01'), date_finish = pd.to_datetime('2017-01-01'), cash_start = 1000000):
        """
            Установка всех переменных (которые меняются при активности) в начальное состояние

        Args:
            cash_start:     Денежные средства стартовые
            date_start:     Дата стартовая

        """

        date = date_start + MonthBegin(n=0)

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

        self.port_df = pd.DataFrame (columns = ['insrt_id', 'quantity'])
        
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
            return True
    
    def calc_data(self):
        """
            Рассчёт данных для периода

        Args:

        Returns:
            True: Если всё отработало правильно

        Raises:
        """
        self.price_hist_df = self.__price_hist_df[(self.__price_hist_df['date'] <= self.date)]
        self.price_df = pd.merge(self.price_hist_df.groupby(['insrt_id'], as_index = False)['date'].max(), self.__price_hist_df, on = ['insrt_id', 'date'])[['insrt_id', 'price']]
        self.spr_df = self.__spr_df[self.__spr_df['insrt_id'].isin(self.price_hist_df['insrt_id'].unique()) == True]
        return True
    
    def buy(self, insrt_id, value):
        """
            Осуществление покупки ПИФа.

        Args:
            insrt_id: 
                Id ПИФа к покупке.
            value: 
                Приведёная стоимость, на которую необходимо приобрести ПИФ.
                Стоимость указывается в деньгах до покупки - на сколько реально уменьшится
                количество денег при покупке.

        Returns:
            True: Если всё отработало правильно

        Raises:
            IOError: 
                There is no such insrt ___ in spr
                Value ___ is less than the minimum sum ___
                There is no such insrt ___ in price
                Value ___ is more than balance of cash ___
        """

        # Получение справочной информации по ПИФу
        spr_insrt_temp_df = self.spr_df[self.spr_df['insrt_id'] == insrt_id]
        if (spr_insrt_temp_df.shape[0] == 0):
            raise Exc('There is no such insrt ' + str(insrt_id) + ' in spr')
        spr_insrt = spr_insrt_temp_df.iloc[0]
        
        # Порверка, что сумма покупки не меньше минимальной в приведёной стоимости
        if (value < spr_insrt.min_sum):
            raise Exc('Value ' + str(value) + ' is less than the minimum sum ' + str(spr_insrt.min_sum))

        # Получение текущей цены по ПИФу
        price_temp_df = self.price_df[(self.price_df['insrt_id'] == insrt_id)]
        if (price_temp_df.shape[0] == 0):
            raise Exc('There is no such pinsrt ' + str(insrt_id) + ' in price')
        price_se = price_temp_df.iloc[0]
        
        # Получение портфельной записи по ПИФу
        port_temp_df = self.port_df[self.port_df['insrt_id'] == insrt_id]
        if (port_temp_df.shape[0] == 0):
            self.port_df = self.port_df.append({'insrt_id': insrt_id, 'quantity': 0}, ignore_index=True)
            self.port_df['insrt_id'] = self.port_df['insrt_id'].astype(int)
            port_temp_df = self.port_df[self.port_df['insrt_id'] == insrt_id]
        port_se = port_temp_df.iloc[0]

        # Вычисление количетсва ПИФов на приобретение
        insrt_quantity = value / (price_se['price'] * (1 + spr_insrt.surcharge))

        # Проверка остатка на балансе кэша
        if (value - self.cash  > 1e-9):
            raise Exc('Value ' + str(value) + ' is more than balance of cash ' + str(self.cash) + '. Difference: ' + str(value - self.cash))
            
        # Покупка
        self.cash = self.cash - value
        self.port_df.set_value(port_se.name, 'quantity',  port_se['quantity'] + insrt_quantity)
            
        return True
    
    def sell(self, insrt_id, value):
        """
            Осуществление продажи ПИФа.

        Args:
            insrt_id: 
                Id ПИФа к продаже.
            value: 
                Приведёная стоимость, на которую необходимо продать ПИФ.
                Стоимость указывается в деньгах после продажи - на сколько реально увеличится
                количество денег при продаже.

        Returns:
            True: Если всё отработало правильно

        Raises:
            IOError: 
                There is no such insrt ___ in spr
                There is no such insrt ___ in price
                There is no such insrt ___ in port
                Value ___ is more than balance of insrt ___
        """

        # Получение справочной информации по ПИФу
        spr_insrt_temp_df = self.spr_df[self.spr_df['insrt_id'] == insrt_id]
        if (spr_insrt_temp_df.shape[0] == 0):
            raise Exc('There is no such insrt ' + str(insrt_id) + ' in spr')
        spr_insrt = spr_insrt_temp_df.iloc[0]
        
        # Получение текущей цены по ПИФу
        price_temp_df = self.price_df[(self.price_df['insrt_id'] == insrt_id)]
        if (price_temp_df.shape[0] == 0):
            raise Exc('There is no such insrt ' + str(insrt_id) + ' in price')
        price_se = price_temp_df.iloc[0]

        # Получение портфельной записи по ПИФу
        port_temp_df = self.port_df[self.port_df['insrt_id'] == insrt_id]
        if (port_temp_df.shape[0] == 0):
            raise Exc('There is no such insrt ' + str(insrt_id) + ' in port')
        port_se = port_temp_df.iloc[0]
            
        #Вычисление количетсва ПИФов на продажу
        insrt_quantity = value / (price_se['price'] * (1 - spr_insrt.discount))
            
        # Проверка остатка на балансе ПИФа
        #if (value > (port_se['quantity'] * price_se['price'] * (1 - spr_insrt.discount))):
        #    raise Exc('Value ' + str(value) + ' is more than balance of insrt ' + str(port_se['quantity'] * (price_se['price'] * (1 - spr_insrt.discount))) + '. Difference: ' + str(value - (port_se['quantity'] * (price_se['price'] * (1 - spr_insrt.discount)))))
        if (insrt_quantity - port_se['quantity'] > 1e-9):
            raise Exc('Value ' + str(value) + ' is more than balance of insrt ' + str(port_se['quantity'] * (price_se['price'] * (1 - spr_insrt.discount))) + '. Difference: ' + str(value - (port_se['quantity'] * (price_se['price'] * (1 - spr_insrt.discount)))))

        # Продажа
        self.port_df.set_value(port_se.name, 'quantity',  port_se['quantity'] - insrt_quantity)
        self.cash = self.cash + value

        return True