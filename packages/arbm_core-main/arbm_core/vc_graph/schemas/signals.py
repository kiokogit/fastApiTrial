import abc
import datetime
import enum

from src import PyMongoBase


class SignalDirection(str, enum.Enum):
    LookingToInvest = "looking_to_invest"
    SeekingInvestment = "seeking_investment"


class SignalSource(str, enum.Enum):
    Linkedin = "linkedin"
    Twitter = "twitter"


class SignalStrength:
    Weak = "weak"
    Strong = "strong"


class Interaction(PyMongoBase, abc.ABC):
    pass


class InvestmentSignal(Interaction):
    direction: SignalDirection
    source: SignalSource
    strength: SignalStrength

    date: datetime.datetime