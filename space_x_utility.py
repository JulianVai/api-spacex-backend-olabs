import pandas as pd
from sqlalchemy import create_engine, String, Float
from sqlalchemy.dialects.mysql import DATETIME
from haversine import haversine


class SpaceXUtil:
    def __init__(self, json_file_name):
        self.raw_df = pd.read_json(json_file_name)
        self.engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                                    .format(user="root",
                                            pw="1234",
                                            db="spacex"))

    def transform_clean_raw(self) -> pd.DataFrame:
        relevant_data = self.raw_df
        relevant_data["creation_date"] = relevant_data.apply(lambda x: x["spaceTrack"]["CREATION_DATE"],
                                                             axis=1)
        relevant_data["object_name"] = relevant_data.apply(lambda x: x["spaceTrack"]["OBJECT_NAME"],
                                                           axis=1)
        relevant_data["object_id"] = relevant_data.apply(lambda x: x["spaceTrack"]["OBJECT_ID"],
                                                         axis=1)

        relevant_data = relevant_data[["creation_date",
                                       "longitude",
                                       "latitude",
                                       "id",
                                       "object_name",
                                       "object_id"]]

        relevant_data.dropna(axis=0, inplace=True)
        relevant_data.reset_index(drop=True, inplace=True)
        return relevant_data

    def load_to_mysql(self) -> None:
        data_l = self.transform_clean_raw()
        data_l.to_sql("spacex_timeline_test",
                      con=self.engine,
                      if_exists="replace",
                      index=False,
                      dtype={"creation_date": DATETIME,
                             "longitude": Float,
                             "latitude": Float,
                             "id": String(64),
                             "object_name": String(64),
                             "object_id": String(64)})

    @staticmethod
    def calc_haversine(row, ref_coord) -> float:
        return haversine(ref_coord, (row["latitude"], row["longitude"]))

    def last_known_position(self, id_) -> dict:
        last_known_q = f"""select spacex_timeline_test.* from spacex_timeline_test, (
            select id,  max(creation_date) as last_record from spacex_timeline_test
            group by id) last_record_id
            where spacex_timeline_test.id=last_record_id.id
            and spacex_timeline_test.creation_date=last_record_id.last_record
            and spacex_timeline_test.id="{id_}"
            limit 1
            """

        return dict(next(self.engine.execute(last_known_q)))

    def closest_sat(self, time_s, ref_lat, ref_long) -> str:
        top_100_q = f"""select * from spacex_timeline_test
            order by abs(timestampdiff(SECOND,creation_date, "{time_s}"))
            limit 100
            """
        ref_i_coord = (ref_lat, ref_long)

        top_df = pd.read_sql(sql=top_100_q,
                             con=self.engine,
                             parse_dates=["creation_date"])
        top_df["h_dist"] = top_df.apply(self.calc_haversine,
                                        ref_coord=ref_i_coord,
                                        axis=1)
        return top_df.sort_values("h_dist").head(1)["id"].values[0]

