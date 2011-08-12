namespace Volante
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// R2 rectangle class. This class is used in spatial index.
    /// </summary>
    public struct RectangleR2
    {
        private double _top;
        private double _left;
        private double _bottom;
        private double _right;

        /// <summary>
        /// Smallest Y coordinate of the rectangle
        /// </summary>
        public double Top
        {
            get
            {
                return _top;
            }
        }

        /// <summary>
        /// Smallest X coordinate of the rectangle
        /// </summary>
        public double Left
        {
            get
            {
                return _left;
            }
        }

        /// <summary>
        /// Greatest Y coordinate  of the rectangle
        /// </summary>
        public double Bottom
        {
            get
            {
                return _bottom;
            }
        }

        /// <summary>
        /// Greatest X coordinate  of the rectangle
        /// </summary>
        public double Right
        {
            get
            {
                return _right;
            }
        }

        /// <summary>
        /// Rectangle area
        /// </summary>
        public double Area()
        {
            return (_bottom - _top) * (_right - _left);
        }

        /// <summary>
        /// Area of covered rectangle for two sepcified rectangles
        /// </summary>
        public static double JoinArea(RectangleR2 a, RectangleR2 b)
        {
            double _left = (a._left < b._left) ? a._left : b._left;
            double _right = (a._right > b._right) ? a._right : b._right;
            double _top = (a._top < b._top) ? a._top : b._top;
            double _bottom = (a._bottom > b._bottom) ? a._bottom : b._bottom;
            return (_bottom - _top) * (_right - _left);
        }


        /// <summary>
        /// Create copy of the rectangle
        /// </summary>
        public RectangleR2(RectangleR2 r)
        {
            this._top = r._top;
            this._left = r._left;
            this._bottom = r._bottom;
            this._right = r._right;
        }

        /// <summary>
        /// Construct rectangle with specified coordinates
        /// </summary>
        public RectangleR2(double _top, double _left, double _bottom, double _right)
        {
            Debug.Assert(_top <= _bottom && _left <= _right);
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
        public void Join(RectangleR2 r)
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
        public static RectangleR2 Join(RectangleR2 a, RectangleR2 b)
        {
            RectangleR2 r = new RectangleR2(a);
            r.Join(b);
            return r;
        }

        /// <summary>
        /// Checks if this rectangle intersects with specified rectangle
        /// </summary>
        public bool Intersects(RectangleR2 r)
        {
            return _left <= r._right && _top <= r._bottom && _right >= r._left && _bottom >= r._top;
        }

        /// <summary>
        /// Checks if this rectangle contains the specified rectangle
        /// </summary>
        public bool Contains(RectangleR2 r)
        {
            return _left <= r._left && _top <= r._top && _right >= r._right && _bottom >= r._bottom;
        }

        /// <summary>
        /// Check if rectanlge is empty 
        /// </summary>
        public bool IsEmpty()
        {
            return _left > _right;
        }
    }
}
