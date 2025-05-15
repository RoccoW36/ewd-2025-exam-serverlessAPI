export type CinemaSchedule = {
  cinemaId: number;
  movieId: string;
  period: string;
  movieTitle: string;
  city: string;
};

 export type CinemaScheduleQueryParams = {
  cinemaId: number;
  movieId?: string;
  period?: string;
  }
