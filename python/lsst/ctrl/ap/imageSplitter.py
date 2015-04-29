from PIL import Image

class ImageSplitter(object):

    def __init__(self, filename=None):
        self.filename = filename
        pass

    def splitToNames(self, names, columns, rows, format="png"):
        images = self.split(columns, rows)
        x = 0
        for name in names:
            images[x].save(name, format)
            x += 1
        images = None

    def split(self, columns, rows):
        im = Image.open(self.filename)

        width, height = im.size

        tiles = columns*rows
        tile_width = width/columns
        tile_height = height/rows

        tiles = len(sensors)

        x = 0
        y = 0
        images = []
        for y in range(0, columns):
            for x in range(0, rows):
                area = (x*tile_width, y*tile_height, x*tile_width+tile_width, y*tile_height++tile_height)
                imageTile = im.crop(area)
                images.append(imageTile)
        return images
