namespace Perst
{
    using System;
    /// <summary>
    /// R2 rectangle class. This class is used in spatial index.
    /// </summary>
    public struct Rectangle  
    {
        private int _top;
        private int _left;
        private int _bottom;
        private int _right;

        /// <summary>
        /// Smallest Y coordinate of the rectangle
        /// </summary>
        public int top 
        { 
            get 
            { 
                return _top;
            }
        }

        /// <summary>
        /// Smallest X coordinate of the rectangle
        /// </summary>
        public int left 
        {
            get 
            { 
                return _left;
            }
        }

        /// <summary>
        /// Greatest Y coordinate  of the rectangle
        /// </summary>
        public int bottom 
        {
            get 
            { 
                return _bottom;
            }
        }

        /// <summary>
        /// Greatest X coordinate  of the rectangle
        /// </summary>
        public int right 
        {
            get 
            { 
                return _right;
            }
        }

        /// <summary>
        /// Rectangle area
        /// </summary>
        public int area() 
        { 
            return (_bottom-_top)*(_right-_left);
        }

        /// <summary>
        /// Area of covered rectangle for two sepcified rectangles
        /// </summary>
        public static int joinArea(Rectangle a, Rectangle b) 
        {
            int _left = (a._left < b._left) ? a._left : b._left;
            int _right = (a._right > b._right) ? a._right : b._right;
            int _top = (a._top < b._top) ? a._top : b._top;
            int _bottom = (a._bottom > b._bottom) ? a._bottom : b._bottom;
            return (_bottom-_top)*(_right-_left);
        }


        /// <summary>
        /// Create copy of the rectangle
        /// </summary>
        public Rectangle(Rectangle r) 
        {
            this._top = r._top;
            this._left = r._left;
            this._bottom = r._bottom;
            this._right = r._right;
        }

        /// <summary>
        /// Construct rectangle with specified coordinates
        /// </summary>
        public Rectangle(int _top, int _left, int _bottom, int _right) 
        { 
            Assert.that(_top <= _bottom && _left <= _right);
            this._top = _top;
            this._left = _left;
            this._bottom = _bottom;
            this._right = _right;
        }

        /// <summary>
        /// Join two rectangles. This rectangle is updates to contain cover of this and specified rectangle.
        /// </summary>
        /// <param name="r">rectangle to be joined with this rectangle
        /// </param>
        public void join(Rectangle r) 
        { 
            if (_left > r._left) 
            { 
                _left = r._left;
            }
            if (_right < r._right) 
            { 
                _right = r._right;
            }
            if (_top > r._top) 
            { 
                _top = r._top;
            }
            if (_bottom < r._bottom) 
            { 
                _bottom = r._bottom;
            }
        }
    

        /// <summary>
        ///  Non destructive join of two rectangles. 
        /// </summary>
        /// <param name="a">first joined rectangle
        /// </param>
        /// <param name="b">second joined rectangle
        /// </param>
        /// <returns>rectangle containing cover of these two rectangles
        /// </returns>
        public static Rectangle join(Rectangle a, Rectangle b) 
        {
            Rectangle r = new Rectangle(a);
            r.join(b);
            return r;
        }

        /// <summary>
        /// Checks if this rectangle intersects with specified rectangle
        /// </summary>
        public bool intersects(Rectangle r) 
        { 
            return _left <= r._right && _top <= r._bottom && _right >= r._left && _bottom >= r._top;
        }

        /// <summary>
        /// Checks if this rectangle contains the specified rectangle
        /// </summary>
        public bool contains(Rectangle r) 
        { 
            return _left <= r._left && _top <= r._top && _right >= r._right && _bottom >= r._bottom;
        }
    }
}
      