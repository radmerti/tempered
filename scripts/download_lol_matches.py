import json
from abc import ABC, abstractmethod
from os import makedirs, remove
from os.path import exists, expanduser, isfile, join

from tempered.limits import RandomizedDurationRateLimit, RateLimit
from tempered.manager import DownloadManager


def get_arguments() -> dict:
    from argparse import ArgumentParser

    parser = ArgumentParser(
        description='Process some integers.')

    parser.add_argument(
        "seed_summoner_name",
        help="Summoner name of some user that is used to seed the search for matches.",
        type=str)

    parser.add_argument(
        "api_key",
        help="API key used for authenticating the requests in the X-Riot-Token header field.",
        type=str)

    parser.add_argument(
        "--min_season",
        help="Matches older than this season are not downloaded.",
        type=int,
        default=11)

    parser.add_argument(
        "-o", "--output_directory",
        help="Path to a directory in which each match will be saved as a json with"
             "file name {gameID}.json. Defaults to '~/Downloads/lol_matches'.",
        type=str,
        default=join(expanduser('~'), 'Downloads', 'lol_matches'))

    parser.add_argument(
        "--region",
        help="League of legends region. For exmaple 'euw1'. Defaults to 'euw1'.",
        type=str,
        default='euw1')

    return parser.parse_args()

class RiotMatchDownloader(DownloadManager):
    def __init__(
            self,
            rate_limits: [RateLimit],
            seed_summoner_name: str,
            api_key: str,
            min_season: int,
            output_directory: str,
            region: str = 'euw1'):
        self.seed_summoner_name = seed_summoner_name
        self.api_key = api_key
        self.min_season = min_season
        self.output_directory = output_directory

        if exists(self.output_directory):
            if isfile(self.output_directory):
                raise RuntimeError("output dir exists and is a file")
        else:
            makedirs(self.output_directory)

        self._headers = { "X-Riot-Token": self.api_key }
        self._base_url = f"https://{region}.api.riotgames.com"

        super().__init__(rate_limits)

    async def _prologue(self):
        # print("_prologue()")
        summoner_url = (f"{self._base_url}/lol/summoner/v4/summoners/by-name/"
                        f"{self.seed_summoner_name}")
        
        summoner = await self.get_json(summoner_url, headers=self._headers)

        await self._tasks.put(summoner['accountId'])

    async def _epilogue(self):
        pass

    async def _handle_task(self, encryptedAccountId):
        print(f"_handle_task({encryptedAccountId})")
        begin_index = 0

        while True:
            matchlist_url = (
                f"{self._base_url}/lol/match/v4/matchlists/by-account/"
                f"{encryptedAccountId}?season={self.min_season}&beginIndex={begin_index}"
            )
            response = await self.get_json(matchlist_url, headers=self._headers)

            for match in response['matches']:

                if match['season'] < self.min_season:
                    return

                match_details = None

                out_path = join(self.output_directory, f"{match['gameId']}.json")
                if exists(out_path):
                    with open(out_path, 'r') as in_file:
                        try:
                            match_details = json.load(in_file)
                        except json.JSONDecodeError as json_error:
                            remove(out_path)
                            print(f"could not decode {out_path}, file deleted")
                
                if match_details is None:
                    match_url = f"{self._base_url}/lol/match/v4/matches/{match['gameId']}"
                    match_details = await self.get_json(match_url, headers=self._headers)

                    print(f"save {match['gameId']}.json")
                    with open(out_path, 'w') as out_file:
                        json.dump(match_details, out_file)

                for participant_identity in match_details['participantIdentities']:
                    await self._tasks.put(participant_identity['player']['currentAccountId'])

            if begin_index == response['endIndex']:
                return

            begin_index = response['endIndex']
                

def main():
    args = get_arguments()
    #  20 requests every 1 seconds
    # 100 requests every 2 minutes
    downloader = RiotMatchDownloader(
        (RandomizedDurationRateLimit(1.0, 20), RandomizedDurationRateLimit(120.0, 100)),
        args.seed_summoner_name,
        args.api_key,
        args.min_season,
        args.output_directory)

    downloader.start()



if __name__ == "__main__":
    main()
