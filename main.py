# https://quantpedia.com/strategies/post-earnings-announcement-drift-combined-with-strong-momentum/
#
# The investment universe consists of all stocks from NYSE, AMEX and NASDAQ with a price greater than $5. Each quarter, all stocks are
# sorted into deciles based on their 12 months past performance. The investor then uses only stocks from the top momentum decile and 
# goes long on each stock 5 days before the earnings announcement and closes the long position at the close of the announcement day. 
# Subsequently, at the close of the announcement day, he/she goes short and he/she closes his short position on the 5th day after the
# earnings announcement.
#
# QC Implementation:
#   - Investment universe consist of stocks with earnings data available.


from pandas.tseries.offsets import BDay
from AlgorithmImports import *
import data_tools

class PostEarningsAnnouncement(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2023, 1, 1)   # earnings days data starts in 2010
        # self.SetEndDate(2018,6,10)
        self.SetCash(100000)
        
        self.quantile:int = 10
        self.period:int = 12 * 21 # need n daily prices
        self.rebalance_period:int = 3 # referes to months, which has to pass, before next portfolio rebalance
        self.leverage:int = 3
        self.free_margin = 1
        self.buyDaysBefore = int(self.GetParameter('buyDaysBefore'))
        self.sellDaysAfter = int(self.GetParameter('sellDaysAfter'))
        self.switchDaysAfter = int(self.GetParameter('switchDaysAfter'))

        self.data:dict[Symbol, data_tools.SymbolData] = {} 
        self.selected_symbols:list[Symbol] = []
        
        # 50 equally weighted brackets for traded symbols
        self.managed_symbols_size:int = 30
        self.managed_symbols:list[data_tools.ManagedSymbol] = []

        self.symbol:Symbol = self.AddEquity('SPY', Resolution.Daily).Symbol
        self.LoadEarningsData()               

        self.months_counter:int = 0
        self.selection_flag:bool = True
        self.UniverseSettings.Resolution = Resolution.Daily
        self.AddUniverse(self.CoarseSelectionFunction)
        
        # Events on earnings days, before and after earning days.

        self.Schedule.On(self.DateRules.MonthStart(self.symbol), self.TimeRules.AfterMarketOpen(self.symbol), self.Selection)

        if self.LiveMode:
            self.Schedule.On(self.DateRules.WeekStart(), self.TimeRules.At(8,0), self.LoadEarningsData)
        
    def LoadEarningsData(self):
        # earning data parsing
        self.Log(f'{self.Time}->Loading Latest Earnings File...')
        self.earnings:dict[datetime.date, list[str]] = {}
        days_before_earnings:list[datetime.date] = []
        
        earnings_set:Set(str) = set()

        if not self.LiveMode:
            earnings_data:str = self.Download('https://www.dropbox.com/scl/fi/t8doznsqhmbby7cxenh0q/earnings_dates_eps.json?rlkey=yh6nhwm8vkby33mz396g0vl8y&dl=1')
        else:
            earnings_data:str = self.Download('https://raw.githubusercontent.com/deerfieldgreen/Post-Earnings-Announcement-Drift-Combined-with-Strong-Momentum/main/earnings_dates_eps_live.json')

        earnings_data_json:list[dict] = json.loads(earnings_data)

        for obj in earnings_data_json:
            date:datetime.date = datetime.strptime(obj['date'], "%Y-%m-%d").date()

            self.earnings[date] = []
            days_before_earnings.append(date - BDay(5))
            
            for stock_data in obj['stocks']:
                ticker:str = stock_data['ticker']

                self.earnings[date].append(ticker)

                earnings_set.add(ticker)


        self.earnings_universe:list[str] = list(earnings_set)
        self.Log(f'{self.Time}->Updated Earnings File with {len(self.earnings_universe)} tickers')

        self.Schedule.On(self.DateRules.On(days_before_earnings), self.TimeRules.AfterMarketOpen(self.symbol), self.DaysBefore)
   
    def OnSecuritiesChanged(self, changes):
        for security in changes.AddedSecurities:
            security.SetFeeModel(data_tools.CustomFeeModel())
            security.SetLeverage(self.leverage)
                
    def CoarseSelectionFunction(self, coarse):
        # daily update of prices
        for stock in coarse:
            symbol:Symbol = stock.Symbol

            if symbol in self.data:
                self.data[symbol].update(stock.AdjustedPrice)
        
        if not self.selection_flag:
            return Universe.Unchanged
        self.selection_flag = False
        
        selected:list[Symbol] = [x.Symbol for x in coarse if x.HasFundamentalData 
                                and x.Market == 'usa' and x.Price > 5
                                and x.Symbol.Value in self.earnings_universe]
                                
        # warm up prices
        for symbol in selected:
            if symbol in self.data:
                continue
        
            self.data[symbol] = data_tools.SymbolData(self.period)
            history = self.History(symbol, self.period, Resolution.Daily)

            if history.empty:
                self.Log(f"Not enough data for {symbol} yet")
                continue

            closes = history.loc[symbol].close
            for _, close in closes.items():
                self.data[symbol].update(close)
        
        # calculate momentum for each stock in self.earnings_universe
        momentum:dict[Symbol, float] = { symbol: self.data[symbol].performance() for symbol in selected if self.data[symbol].is_ready() }
                
        if len(momentum) < self.quantile:
            self.selected_symbols = []
            return Universe.Unchanged
            
        quantile:int = int(len(momentum) / self.quantile)
        sorted_by_mom:list[Symbol] = [x[0] for x in sorted(momentum.items(), key=lambda item: item[1])]
        # the investor uses only stocks from the top momentum quantile
        self.selected_symbols = sorted_by_mom[-quantile:]
        
        return self.selected_symbols

    def DaysBefore(self):
        # every day check if x days from now is any earnings day
        earnings_date:datetime.date = (self.Time + BDay(self.buyDaysBefore)).date()
        date_to_liquidate:datetime.date = (earnings_date + BDay(self.sellDaysAfter)).date()
        date_to_switch:datetime.date = (earnings_date + BDay(self.switchDaysAfter)).date()
        self.Log(f'Checking for earnings date: {earnings_date}')
        
        if earnings_date not in self.earnings:
            return

        for symbol in self.selected_symbols:
            ticker:str = symbol.Value
            # is there any symbol which has earnings in x days
            if ticker not in self.earnings[earnings_date]:
                continue

            if (len(self.managed_symbols) < self.managed_symbols_size) and not self.Securities[symbol].Invested and \
                self.Securities[symbol].Price != 0 and self.Securities[symbol].IsTradable:
                self.SetHoldings(symbol, (self.leverage - self.free_margin) / self.managed_symbols_size)
                
                # NOTE: Must offset date to switch position by one day due to midnight execution of OnData function.
                # Alternatively, there's is a possibility to switch to BeforeMarketClose function.
                self.managed_symbols.append(data_tools.ManagedSymbol(symbol, date_to_switch, date_to_liquidate))
                self.Log(f'Position For {symbol.Value}, Earnings Date: {earnings_date}, Switch Date: {date_to_switch}, Liquidation: {date_to_liquidate}')
                    
    def OnData(self, data):
        # switch positions on earnings days.
        curr_date:datetime.date = self.Time.date()
        
        managed_symbols_to_delete:list[data_tools.ManagedSymbol] = []
        for managed_symbol in self.managed_symbols:
            if managed_symbol.date_to_switch <= curr_date and (curr_date < managed_symbol.date_to_liquidate) and \
                self.Portfolio[managed_symbol.symbol].IsLong:
                # switch position from long to short
                self.SetHoldings(managed_symbol.symbol, -1*(self.leverage - self.free_margin) / self.managed_symbols_size)
            
            elif managed_symbol.date_to_liquidate <= curr_date:
                self.Liquidate(managed_symbol.symbol)
                managed_symbols_to_delete.append(managed_symbol)
                
        # remove symbols from management
        for managed_symbol in managed_symbols_to_delete:
            self.managed_symbols.remove(managed_symbol)
            
    def Selection(self):
        # quarter selection
        if self.months_counter % self.rebalance_period == 0:
            self.selection_flag = True
        self.months_counter += 1
