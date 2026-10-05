from durable_actors import Actor, persisted


class BankAccount(Actor):
    _balance: int = persisted(0)

    def get_balance(self) -> int:
        return self._balance

    def deposit(self, amount: int) -> int:
        if amount <= 0:
            raise ValueError("Deposit must be a positive amount in cents")

        self._balance += amount
        return self._balance
