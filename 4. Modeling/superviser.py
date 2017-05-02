import pandas as pd
from pandas.tseries.offsets import *

class Exc(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Superviser:
    
    envir = None       # Окружающая среда
    agent = None       # Агент
    
    def __init__(self, envir, agent):
        
        self.envir = envir
        self.agent = agent
        self.envir.suvis = self
        self.agent.suvis = self

    def show_port(self):
        """
            Отображение полной информации по портфелю.

        Args:

        Returns:
            DataFrame: Полная информация о портфеле со следующими полями:
                pif_id
                company
                pif
                value_pif
                value
                part
        Raises:
        """

        port_temp_df = pd.merge (self.envir.port_df, self.envir.spr_df, on = ['pif_id'])
        port_temp_df = pd.merge (port_temp_df, self.envir.price_df, on = ['pif_id'])
        port_temp_df['value_pif'] = port_temp_df['quantity'] * port_temp_df['price']
        port_temp_df['value_pif'] = port_temp_df['value_pif'].astype(int)
        port_temp_df['value'] = port_temp_df['quantity'] * port_temp_df['price'] * (1-port_temp_df['discount'])
        port_temp_df['part'] = port_temp_df['value']/(port_temp_df['value'].sum())
        port_temp_df = port_temp_df[['pif_id', 'company', 'pif', 'value_pif', 'value', 'part']]
        return port_temp_df

        

    def show_port_sum(self):
        """
            Отображение полной стоимости по портфелю.

        Args:

        Returns:
            float: Полная информация стомиости портфеля.
        Raises:
        """

        port_temp_df = pd.merge (self.envir.port_df, self.envir.spr_df, on = ['pif_id'])
        port_temp_df = pd.merge (port_temp_df, self.envir.price_df, on = ['pif_id'])
        port_temp_df['value'] = port_temp_df['quantity'] * port_temp_df['price'] * (1-port_temp_df['discount'])
        return port_temp_df['value'].sum()

    def action(self):
        self.envir.start()
        self.agent.start()
        
        self.agent.action()


    def action_finish(self):

        # Сохранение истории цен
        writer = pd.ExcelWriter('..\\5. Evaluation\\price_hist.xlsx')
        self.agent.price_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории портфеля
        writer = pd.ExcelWriter('..\\5. Evaluation\\port_hist.xlsx')
        self.agent.port_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории полных данных портфеля с долями
        writer = pd.ExcelWriter('..\\5. Evaluation\\port_data_hist.xlsx')
        self.agent.port_data_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории параметров            
        writer = pd.ExcelWriter('..\\5. Evaluation\\param_hist.xlsx')
        self.agent.param_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории требуемых долей портфеля
        writer = pd.ExcelWriter('..\\5. Evaluation\\port_req_hist.xlsx')
        self.agent.part_req_hist_df.to_excel(writer, 'Данные')
        writer.save()

        