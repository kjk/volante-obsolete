namespace Volante
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Rectangle with integer coordinates. This class is used in spatial index.
    /// </summary>
    public struct Rectangle
    {
        private int top;
        private int left;
        private int bottom;
        private int right;

        /// <summary>
        /// Smallest Y coordinate of the rectangle
        /// </summary>
        public int Top
        {
            get
            {
                return top;
            }
        }

        /// <summary>
        /// Smallest X coordinate of the rectangle
        /// </summary>
        public int Left
        {
            get
            {
                return left;
            }
        }

        /// <summary>
        /// Greatest Y coordinate  of the rectangle
        /// </summary>
        public int Bottom
        {
            get
            {
                return bottom;
            }
        }

        /// <summary>
        /// Greatest X coordinate  of the rectangle
        /// </summary>
        public int Right
        {
            get
            {
                return right;
            }
        }

        /// <summary>
        /// Rectangle area
        /// </summary>
        public long Area()
        {
            return (long)(bottom - top) * (right - left);
        }

        /// <summary>
        /// Area of covered rectangle for two sepcified rectangles
        /// </summary>
        public static long JoinArea(Rectangle a, Rectangle b)
        {
            int left = (a.left < b.left) ? a.left : b.left;
            int right = (a.right > b.right) ? a.right : b.right;
            int top = (a.top < b.top) ? a.top : b.top;
            int bottom = (a.bottom > b.bottom) ? a.bottom : b.bottom;
            return (long)(bottom - top) * (right - left);
        }

        /// <summary>
        /// Create copy of the rectangle
        /// </summary>
        public Rectangle(Rectangle r)
        {
            this.top = r.top;
            this.left = r.left;
            this.bottom = r.bottom;
            this.right = r.right;
        }

        /// <summary>
        /// Construct rectangle with specified coordinates
        /// </summary>
        public Rectangle(int top, int left, int bottom, int right)
        {
            Debug.Assert(top <= bottom && left <= right);
            this.top = top;
            this.left = left;
            this.bottom = bottom;
            this.right = right;
        }

        /// <summary>
        /// Join two rectangles. This rectangle is updates to contain cover of this and specified rectangle.
        /// </summary>
        /// <param name="r">rectangle to be joined with this rectangle
        /// </param>
        public void Join(Rectangle r)
        {
            if (left > r.left)
                left = r.left;

            if (right < r.right)
                right = r.right;

            if (top > r.top)
                top = r.top;

            if (bottom < r.bottom)
                bottom = r.bottom;
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
        public static Rectangle Join(Rectangle a, Rectangle b)
        {
            Rectangle r = new Rectangle(a);
            r.Join(b);
            return r;
        }

        /// <summary>
        /// Checks if this rectangle intersects with specified rectangle
        /// </summary>
        public bool Intersects(Rectangle r)
        {
            return left <= r.right && top <= r.bottom && right >= r.left && bottom >= r.top;
        }

        /// <summary>
        /// Checks if this rectangle contains the specified rectangle
        /// </summary>
        public bool Contains(Rectangle r)
        {
            return left <= r.left && top <= r.top && right >= r.right && bottom >= r.bottom;
        }

        /// <summary>
        /// Check if rectanlge is empty 
        /// </summary>
        public bool IsEmpty()
        {
            return left > right;
        }

        public bool EqualsTo(Rectangle other)
        {
            if (Left != other.Left)
                return false;
            if (Right != other.Right)
                return false;
            if (Top != other.Top)
                return false;
            if (Bottom != other.Bottom)
                return false;
            return true;
        }
    }
}
