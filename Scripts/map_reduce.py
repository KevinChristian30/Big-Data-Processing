from mrjob.job import MRJob, MRStep

class CountMovieByRating(MRJob):

    def mapping(self, key, value):
        userID, movieID, rating, timeStamp = value.split('\t')
        yield rating, 1

    def reducing(self, key, value):
        yield key, sum(value)

    # Override Method dari MRJob
    def steps(self):
        return [
            MRStep(mapper = self.mapping, 
                    reducer = self.reducing)
            ]

CountMovieByRating.run()