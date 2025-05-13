from .france import AnnuaireEntrepriseFetcher


class OfficialRegisterFetcherFactory:
    _map = {
        "FR": AnnuaireEntrepriseFetcher,
    }

    @staticmethod
    def get_fetcher(country: str):
        fetcher_class = OfficialRegisterFetcherFactory._map.get(country)
        if not fetcher_class:
            raise ValueError(f"No fetcher for country: {country}")
        return fetcher_class()
