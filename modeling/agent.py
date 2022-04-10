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
        param_hist_df:    Прогнозы инструментов история
        param_df:         Прогнозы инструментов
        port_summ_value:  Суммарная стоимость портфеля без учёта наличных (приведённая)
        part_curr_df:     Текущие доли инструментов в портфеле В ЦЕНАХ инструмент
        part_req_hist_df: Требуемые доли инструментов в портфеле история
        part_req_df:      Требуемые доли инструментов в портфеле
        port_hist_df:     Портфель инструментов история
        price_hist_df:    Цены инструментов на дату
        port_data_df:     Данные портфеля инструментов
        port_data_hist_df:Данные портфеля инструментов история
        min_thres:        Минимальный порог прибыльности инструмента в разах (1 - это хододность больше нуля)
        top_thres:        Количественный порог прибыльности инструмента (топ рейтинга по прибыльности)
        prim_state:       Состояние первичной закупки инструментов
        den_pow:          Знаменатель степени 1/2**index/den_pow в которую возводится волатильность 
        horizon_days:     Горизонт прогнозирования рабочих дней
        min_hist_days:    Минимальное количество рабочих дней, для которых возможно посчитать доходность за минимальный период
        min_rent_days:    Минимальный период расчёта рентабельности для последующего перевода в годовую рентабельность

    """

    def __init__(self, envir):
        self.envir = envir
        self.envir.agent = self

        self.start()

    def start(
        self,
        top_thres=5,
        min_thres=1,
        pow_st_dev=0.5,
        den_pow=248,
        horizon_days=248,
        min_hist_days=248,
        min_rent_days=248,
    ):
        """
            Установка всех переменных (которые меняются при активности) в начальное состояние

        Args:

        """
        self.param_hist_df = pd.DataFrame(
            columns=["date", "instr_id", "profit_mean", "total_1", "profit_std", "total_2"]
        )
        self.part_req_hist_df = pd.DataFrame(columns=["date", "instr_id", "part"])
        self.port_hist_df = pd.DataFrame(columns=["date", "instr_id", "quantity"])
        self.price_hist_df = pd.DataFrame(columns=["date", "instr_id", "price"])
        self.port_data_hist_df = pd.DataFrame(
            columns=[
                "date",
                "instr_id",
                "quantity",
                "manager",
                "instr",
                "min_sum",
                "surcharge",
                "discount",
                "price",
                "value",
                "part",
            ]
        )

        self.min_thres = min_thres
        self.top_thres = top_thres
        self.pow_st_dev = pow_st_dev
        self.prim_state = True
        self.den_pow = den_pow
        self.horizon_days = horizon_days
        self.min_hist_days = min_hist_days
        self.min_rent_days = min_rent_days

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
        # print()
        # print('#=====================================================================================================')
        # print(self.envir.date, end = ' ')

        self.calc_param()  # Запускать только 1 раз за 1 дату - иначе данные дублируются

        self.chang_port()

    def calc_param(self):
        """
        Расчёт показателей инструментов: средняя доходность, волатильность и т.д.
        """

        # Цикл обхода инструментов по справочнику
        for index, spr_se in self.envir.spr_df.iterrows():

            # gather
            price_hist_temp_df = self.gather(instr_id=spr_se["instr_id"])

            # Если история собрана менее чем за 2 года, то не рассматривать инвестицию
            if len(price_hist_temp_df) < (self.min_hist_days + self.min_rent_days):
                continue

            # preprocess
            price_hist_temp_df = self.preprocess(price_hist_temp_df)
            # print('price_hist_temp_df')
            # print(price_hist_temp_df)

            # fit
            price_hist_temp_df = self.fit(price_hist_temp_df)



            # predict
            profit_mean, profit_std = self.predict(price_hist_temp_df)

            # Поправка на вознаграждения управляющих
            profit_mean = profit_mean * (1 - spr_se["fee"])

            total_1 = profit_mean / (1 + spr_se["surcharge"]) * (1 - spr_se["discount"])
            # соотношение прибыли к риску
            total_2 = total_1 / (profit_std ** self.pow_st_dev)

            # Сохранение в историю текущих параметров
            self.param_hist_df = pd.concat(
                [
                    self.param_hist_df,
                    pd.DataFrame.from_dict(
                        [
                            {
                                "date": self.envir.date,
                                "instr_id": spr_se["instr_id"],
                                "profit_mean": profit_mean,
                                "total_1": total_1,
                                "profit_std": profit_std,
                                "total_2": total_2,
                            }
                        ]
                    ),
                ]
            )

        # Получение текущих параметров обратно из истории
        self.param_df = self.param_hist_df[self.param_hist_df["date"] == self.envir.date]

        # Сохранение в историю текущего портфеля
        port_temp_df = self.envir.port_df.copy()
        port_temp_df["date"] = self.envir.date
        self.port_hist_df = pd.concat([self.port_hist_df, port_temp_df])

        # Сохранение в историю текущих цен
        price_temp_df = self.envir.price_df.copy()
        price_temp_df["date"] = self.envir.date
        self.price_hist_df = pd.concat([self.price_hist_df, price_temp_df])

        # Сохранение в историю полных данных портфеля с долями
        port_data_df = pd.merge(self.envir.port_df, self.envir.spr_df, on=["instr_id"])
        port_data_df = pd.merge(port_data_df, self.envir.price_df, on=["instr_id"])
        port_data_df["value"] = port_data_df["quantity"] * port_data_df["price"] * (1 - port_data_df["discount"])
        port_data_df["part"] = (
            0 if port_data_df["value"].sum() == 0 else port_data_df["value"] / port_data_df["value"].sum()
        )
        # port_data_df['date'] = self.envir.date
        self.port_data_df = port_data_df

        port_data_temp_df = self.port_data_df.copy()
        port_data_temp_df["date"] = self.envir.date
        self.port_data_hist_df = pd.concat([self.port_data_hist_df, port_data_temp_df])

        # Подсчёт текущей суммарной стоимости портфеля в приведённых ценах
        self.port_summ_value = port_data_df["value"].sum()

        return True

    def chang_port(
        self, cash=1000000
    ):  # Данная величина cash устанволена и при первичном закупе у envir. Менять синхорнно
        """
        Расчёт и при необходимости изменение долей в инвестиционном портфеле
        """

        # Вычислить текущие доли паёв В ЦЕНАХ инструмент
        part_curr_df = pd.merge(self.envir.port_df, self.envir.spr_df, on=["instr_id"])
        part_curr_df = pd.merge(part_curr_df, self.envir.price_df, on=["instr_id"])
        part_curr_df["value"] = part_curr_df["quantity"] * part_curr_df["price"] * (1 - part_curr_df["discount"])
        part_curr_df["part"] = (
            0 if part_curr_df["value"].sum() == 0 else part_curr_df["value"] / part_curr_df["value"].sum()
        )

        # print()
        # print('портфель текущий  \n', part_curr_df)

        part_curr_df = part_curr_df[["instr_id", "part"]]
        # Текущие доли паёв В ЦЕНАХ инструмент
        # Поля: instr_id, part
        self.part_curr_df = part_curr_df

        # print()
        # print('текущие прогнозы по инструментам \n', self.param_df)

        # Оставить инструменты выше порога прибыльности
        part_req_df = self.param_df[self.param_df["total_2"] > self.min_thres]
        part_req_df = (
            part_req_df.sort_values(by=["total_2"], ascending=False).reset_index(drop=True).loc[0 : self.top_thres - 1]
        )

        # Вычислить требуемые доли инструментов
        part_req_df.loc[:, "bet_thres"] = part_req_df["total_2"] - self.min_thres
        part_req_df["bet_thres^pow"] = part_req_df["bet_thres"] ** 1
        part_req_df["part"] = (
            0 if part_req_df["total_2"].sum() == 0 else part_req_df["total_2"] / part_req_df["total_2"].sum()
        )
        # part_req_df['part'] = (0 if part_req_df['bet_thres^pow'].sum() == 0 else part_req_df['bet_thres^pow'] / part_req_df['bet_thres^pow'].sum()) #part_req_df['bet_thres'] / part_req_df['bet_thres'].sum()

        # print()
        # print ('текущие отбранные инструменты: \n', part_req_df)

        part_req_df = part_req_df[["instr_id", "bet_thres^pow", "part", "total_2"]]
        # Требуемые доли паёв В ЦЕНАХ инструмент
        # Поля: instr_id, part
        self.part_req_df = part_req_df

        # Сохранить в историю требуемые доли паёв В ЦЕНАХ инструмент
        part_req_temp_df = part_req_df
        part_req_temp_df["date"] = self.envir.date
        # История требуемых долей паёв В ЦЕНАХ инструмент
        # Поля: date, instr_id, part
        # self.part_req_hist_df = self.part_req_hist_df.append(part_req_temp_df, ignore_index = True)
        self.part_req_hist_df = pd.concat([self.part_req_hist_df, part_req_temp_df])

        # Вычислить доли паёв, которые необходимо купить/продать
        port_chang_df = pd.merge(
            pd.DataFrame(part_curr_df[["instr_id", "part"]].values, columns=["instr_id", "part_curr"]),
            pd.DataFrame(part_req_df[["instr_id", "part"]].values, columns=["instr_id", "part_req"]),
            how="outer",
            on="instr_id",
        ).fillna(value=0)

        if port_chang_df.shape[0] == 0:
            port_chang_df["part_sell"] = None
            port_chang_df["part_buy"] = None
        else:
            port_chang_df["part_sell"] = port_chang_df.apply(
                lambda row: (row["part_curr"] - row["part_req"]) if (row["part_curr"] > row["part_req"]) else 0, axis=1
            )
            port_chang_df["part_buy"] = port_chang_df.apply(
                lambda row: (row["part_req"] - row["part_curr"]) if (row["part_curr"] < row["part_req"]) else 0, axis=1
            )
        # Доли паёв, которые необходимо купить/продать
        # Поля: instr_id, part_curr, part_req, part_sell, part_buy
        self.port_chang_df = port_chang_df

        # print()
        # print('Необходимые изменения в портфеле \n', port_chang_df)

        if self.prim_state:  # Если это первичная закупка инструментов
            cash_temp = cash

            # Проверка на соответствие требованиям минимальной стоимости приобретения , и пересчёт долей при необходимости
            part_req_df = pd.merge(part_req_df, self.envir.spr_df[["instr_id", "min_sum"]], on="instr_id")
            part_req_df["value_buy"] = part_req_df["part"] * cash_temp
            part_req_df = part_req_df[part_req_df["value_buy"] >= part_req_df["min_sum"]]
            part_req_df["part"] = (
                0 if part_req_df["total_2"].sum() == 0 else part_req_df["total_2"] / part_req_df["total_2"].sum()
            )  # part_req_df['bet_thres'] / part_req_df['bet_thres'].sum()
            # part_req_df['part'] = (0 if part_req_df['bet_thres^pow'].sum() == 0 else part_req_df['bet_thres^pow'] / part_req_df['bet_thres^pow'].sum()) #part_req_df['bet_thres'] / part_req_df['bet_thres'].sum()

            # print()
            # print('part_req_df \n', part_req_df)

            part_req_df = part_req_df[["instr_id", "part"]]

            for index, row_buy in part_req_df.iterrows():
                part_temp = row_buy["part"]

                # Проверка на соответствие требованиям минимальной стоимости приобретения инструмента
                if (
                    part_temp * cash_temp
                    > self.envir.spr_df[self.envir.spr_df["instr_id"] == row_buy["instr_id"]].iloc[0]["min_sum"]
                ):

                    # Покупка инструмента
                    self.buy(row_buy["instr_id"], part_temp * cash_temp)
                    self.prim_state = False
        else:
            # Рассчитать матрицу выгодности перекупки паёв
            # Выгода от смены паёв
            self.benef_shar = pd.DataFrame(columns=["instr_id_sell", "instr_id_buy", "benef"])
            port_ch_param_df = pd.merge(port_chang_df, self.param_df[["instr_id", "total_2"]], on="instr_id")
            port_ch_param_df = pd.merge(
                port_ch_param_df, self.envir.spr_df[["instr_id", "surcharge", "discount"]], on="instr_id"
            )
            # Доли паёв, которые необходимо купить/продать с параметрами
            # Поля: instr_id, part_curr, part_req, part_sell, part_buy, profit_mean
            self.port_ch_param_df = port_ch_param_df

            # Распечатка всех показателей
            # print ('Смена портфеля: \n', port_chang_df)
            # print ('Параметры инструментов: \n', port_ch_param_df)

            # Цикл расчёта выгодности перехода по необходимым инструментам
            for index_sell in port_ch_param_df[
                port_ch_param_df["part_req"] < port_ch_param_df["part_curr"]
            ].index:  # После отладки убрать объединение со справочником
                instr_sell = port_ch_param_df.loc[index_sell]
                for index_buy in port_ch_param_df[port_ch_param_df["part_req"] > port_ch_param_df["part_curr"]].index:
                    instr_buy = port_ch_param_df.loc[index_buy]
                    benef = instr_buy["total_2"] - (
                        instr_sell["total_2"] * (1 + instr_sell["surcharge"]) / (1 - instr_sell["discount"])
                    )  # Т.к. в total_2 скидка и надбавка уже учтены, то применяем их обратно

                    # print ('instr_sell: ', instr_sell['instr_id'], 'instr_buy: ', instr_buy['instr_id'], 'benef: ', benef)
                    self.benef_shar = self.benef_shar.append(
                        {
                            "instr_id_sell": instr_sell["instr_id"],
                            "instr_id_buy": instr_buy["instr_id"],
                            "benef": benef,
                        },
                        ignore_index=True,
                    )
            self.benef_shar = self.benef_shar[self.benef_shar["benef"] > 0].sort_values(by="benef", ascending=False)

            # Продать/купить доли паёв
            # Таблица у которой будут учитываться (убавляться) доли для смены
            temp_chang_df = port_chang_df[["instr_id", "part_sell", "part_buy"]]
            self.temp_chang_df = temp_chang_df
            for index in self.benef_shar.index:
                # print (self.benef_shar.loc[index])
                row_sell = temp_chang_df[temp_chang_df["instr_id"] == self.benef_shar.loc[index]["instr_id_sell"]].iloc[
                    0
                ]
                row_buy = temp_chang_df[temp_chang_df["instr_id"] == self.benef_shar.loc[index]["instr_id_buy"]].iloc[0]
                # print('part_sell: ', row_sell['part_sell'], 'index: ', row_sell.name)
                # print('part_buy: ',  row_buy['part_buy'], 'index: ', row_buy.name)

                # Выбираем меньшую долю из необходимых покупки и продажи
                if row_sell["part_sell"] > row_buy["part_buy"]:
                    # print ('buy smaller')
                    part_temp = row_buy["part_buy"]
                else:
                    # print ('sell smaller')
                    part_temp = row_sell["part_sell"]

                # Проверка на соответствие требованиям минимальной стоимости приобретения инструмента
                if (
                    part_temp * self.port_summ_value
                    > self.envir.spr_df[self.envir.spr_df["instr_id"] == row_buy["instr_id"]].iloc[0]["min_sum"]
                ):

                    # Продажа инструмента
                    self.sell(row_sell["instr_id"], part_temp * self.port_summ_value)

                    # temp_chang_df.loc[row_sell.name]['part_sell'] = row_sell['part_sell'] - part_temp
                    temp_chang_df.set_value(row_sell.name, "part_sell", row_sell["part_sell"] - part_temp)

                    # исправление ошибки float
                    if temp_chang_df.loc[row_sell.name]["part_sell"] < 1e-9:
                        temp_chang_df.loc[row_sell.name]["part_sell"] = 0

                    # Покупка инструмента
                    self.buy(row_buy["instr_id"], part_temp * self.port_summ_value)
                    temp_chang_df.loc[row_buy.name]["part_buy"] = row_buy["part_buy"] - part_temp
                    # исправление ошибки float
                    if temp_chang_df.loc[row_sell.name]["part_buy"] < 1e-9:
                        temp_chang_df.loc[row_sell.name]["part_buy"] = 0

    def gather(self, instr_id):
        # Получение истории цен инструмента отсортированной по дате
        price_hist_temp_df = (
            self.envir.price_hist_df[self.envir.price_hist_df["instr_id"] == instr_id]
                .sort_values(by="date") #, ascending=False
                .reset_index(drop=True)
        )
        return price_hist_temp_df

    def preprocess(self, price_hist_temp_df):

        # Непосредственный рассчёт показателей
        price_hist_temp_df["price_next"] = price_hist_temp_df["price"].shift(
            -self.horizon_days
        )

        price_hist_temp_df = price_hist_temp_df[
            price_hist_temp_df["price_next"].notnull()
            & (price_hist_temp_df.index >= price_hist_temp_df.index.max() - 2 * (self.min_hist_days + self.min_rent_days))
        ]  # Берётся максимальная история в 2 раза длиннее минимальной

        # рассчитываем доходность на горизонт вперёд
        price_hist_temp_df["profit"] = price_hist_temp_df["price_next"] / price_hist_temp_df["price"]
        return price_hist_temp_df

    def fit(self, price_hist_temp_df):
        # расчёт sample weight для среднего всзвешенного
        date_max = price_hist_temp_df["date"].max()
        price_hist_temp_df["diff_day"] = (price_hist_temp_df["date"].apply(lambda x: x - date_max)).dt.days #envir.date
        price_hist_temp_df['power'] = -price_hist_temp_df['diff_day'] / 365 #agent.den_pow
        # price_hist_temp_df["power"] = price_hist_temp_df.index / self.den_pow
        price_hist_temp_df["weight"] = 0.5 ** price_hist_temp_df["power"]  # за den_pow вес строки падает до 0.5
        price_hist_temp_df["weight_rel"] = price_hist_temp_df["weight"] / (price_hist_temp_df["weight"].sum())
        price_hist_temp_df["weight_profit"] = price_hist_temp_df["weight_rel"] * np.log(price_hist_temp_df["profit"])

        price_hist_temp_df["dif"] = price_hist_temp_df["profit"] / gmean(
            price_hist_temp_df["profit"]
        )  # во сколько годовая (за период) прибыль больше среднегеомертрической
        price_hist_temp_df["ln_dif"] = np.log(price_hist_temp_df["dif"])
        price_hist_temp_df["ln_dif_2"] = price_hist_temp_df["ln_dif"] ** 2
        price_hist_temp_df["ln_dif_2_weight"] = price_hist_temp_df["weight_rel"] * price_hist_temp_df["ln_dif_2"]
        return price_hist_temp_df

    def predict(self, price_hist_temp_df):

        # Расчёт итоговых сводных показателей (средних всзвешенных)
        # прибыль - среднее
        profit_mean = np.exp(price_hist_temp_df["weight_profit"].sum())
        # прибыль - стандартное отклонение
        profit_std = np.exp((price_hist_temp_df["ln_dif_2_weight"].sum()) ** 0.5)

        return profit_mean, profit_std

    def buy(self, instr_id, value):

        # исправление ошибки float
        if ((value - self.envir.cash) < 1e-9) & ((value - self.envir.cash) > -1e-9):
            value = self.envir.cash

        self.envir.buy(instr_id, value)
        return True

    def sell(self, instr_id, value):

        # Получение справочной информации по инструменту
        spr_instr_temp_df = self.envir.spr_df[self.envir.spr_df["instr_id"] == instr_id]
        if spr_instr_temp_df.shape[0] == 0:
            raise Exc("There is no such instr " + str(instr_id) + " in spr")
        spr_instr = spr_instr_temp_df.iloc[0]

        # Получение текущей цены по инструменту
        price_temp_df = self.envir.price_df[(self.envir.price_df["instr_id"] == instr_id)]
        if price_temp_df.shape[0] == 0:
            raise Exc("There is no such instr " + str(instr_id) + " in price")
        price_se = price_temp_df.iloc[0]

        # Получение портфельной записи по инструменту
        port_temp_df = self.envir.port_df[self.envir.port_df["instr_id"] == instr_id]
        if port_temp_df.shape[0] == 0:
            raise Exc("There is no such instr " + str(instr_id) + " in port")
        port_se = port_temp_df.iloc[0]

        # Вычисление количетсва инструментов на продажу
        instr_quantity = value / (price_se["price"] * (1 - spr_instr.discount))

        # исправление ошибки float
        # if ((value - (port_se['quantity'] * price_se['price'] * (1 - spr_instr.discount))) < 1e-9) & ((value - (port_se['quantity'] * price_se['price'] * (1 - spr_instr.discount))) > -1e-9):
        #    value = port_se['quantity'] * price_se['price'] * (1 - spr_instr.discount)
        if (port_se["quantity"] - instr_quantity < 1e-9) & (port_se["quantity"] - instr_quantity > -1e-9):
            value = port_se["quantity"] * (price_se["price"] * (1 - spr_instr.discount))

        self.envir.sell(instr_id, value)
        return True
