import pandas as pd
from pandas.tseries.offsets import *
from os import listdir
from os.path import isfile, join

class Exc(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

class Superviser:
    """
    Учитель

    Attributes:

        type_instr:    Тип инстурмента инвестиций
    """
    
    envir = None       # Окружающая среда
    agent = None       # Агент
    
    def __init__(self, envir, agent, type_instr):
        
        self.envir = envir
        self.agent = agent
        self.envir.suvis = self
        self.agent.suvis = self
        self.type_instr = type_instr

    def show_port(self):
        """
            Отображение полной информации по портфелю.

        Args:

        Returns:
            DataFrame: Полная информация о портфеле со следующими полями:
                instr_id
                manager
                instr
                value_instr
                value
                part
        Raises:
        """

        port_temp_df = pd.merge (self.envir.port_df, self.envir.spr_df, on = ['instr_id'])
        port_temp_df = pd.merge (port_temp_df, self.envir.price_df, on = ['instr_id'])
        port_temp_df['value_instr'] = port_temp_df['quantity'] * port_temp_df['price']
        port_temp_df['value_instr'] = port_temp_df['value_instr'].astype(int)
        port_temp_df['value'] = port_temp_df['quantity'] * port_temp_df['price'] * (1-port_temp_df['discount'])
        port_temp_df['part'] = port_temp_df['value']/(port_temp_df['value'].sum())
        port_temp_df = port_temp_df[['instr_id', 'manager', 'instr', 'value_instr', 'value', 'part']]
        return port_temp_df


    def convert_xlsx(self, path):
        files = [f for f in listdir(path) if isfile(join(path, f))]

        for file in files:
            file_split = file.split('.')
            ext = file_split[len(file_split)-1]
            name = file_split[:len(file_split)-1]
            if ((ext == 'xls') & (file[0] != '~')):
                init_df = pd.read_excel(path + file)
                writer = pd.ExcelWriter(path + ''.join(name) + '.xlsx')
                init_df.to_excel(writer, 'Данные', index = False)
                writer.save()

    def show_port_sum(self):
        """
            Отображение полной стоимости по портфелю.

        Args:

        Returns:
            float: Полная информация стомиости портфеля.
        Raises:
        """

        port_temp_df = pd.merge (self.envir.port_df, self.envir.spr_df, on = ['instr_id'])
        port_temp_df = pd.merge (port_temp_df, self.envir.price_df, on = ['instr_id'])
        port_temp_df['value'] = port_temp_df['quantity'] * port_temp_df['price'] * (1-port_temp_df['discount'])
        return port_temp_df['value'].sum()

    def action(self):
        self.envir.start()
        self.agent.start()
        
        self.agent.action()


    def action_finish(self):

        # Сохранение истории цен
        writer = pd.ExcelWriter('..\\5. Evaluation\\' + self.type_instr + '\\price_hist.xlsx')
        self.agent.price_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории портфеля
        writer = pd.ExcelWriter('..\\5. Evaluation\\' + self.type_instr + '\\port_hist.xlsx')
        self.agent.port_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории полных данных портфеля с долями
        writer = pd.ExcelWriter('..\\5. Evaluation\\' + self.type_instr + '\\port_data_hist.xlsx')
        self.agent.port_data_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории параметров            
        writer = pd.ExcelWriter('..\\5. Evaluation\\' + self.type_instr + '\\param_hist.xlsx')
        self.agent.param_hist_df.to_excel(writer, 'Данные')
        writer.save()

        # Сохранение истории требуемых долей портфеля
        writer = pd.ExcelWriter('..\\5. Evaluation\\' + self.type_instr + '\\port_req_hist.xlsx')
        self.agent.part_req_hist_df.to_excel(writer, 'Данные')
        writer.save()

        