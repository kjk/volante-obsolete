namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
    using System.Diagnostics;
	
    class RtreePage:Persistent 
    {
        internal struct Branch  
        { 
            internal Rectangle   r;
            internal IPersistent p;

            internal Branch(Rectangle r, IPersistent p) 
            { 
                this.r = r;
                this.p = p;
            }
        };

        internal const int card = (Page.pageSize-ObjectHeader.Sizeof-4-4)/(5*4);
        internal const int minFill = card/2;

        internal int      n;
        internal Branch[] b;

        internal RtreePage(IPersistent obj, Rectangle r) 
        {
            b = new Branch[card];
            n = 1;
            b[0] = new Branch(r, obj);
            for (int i = 1; i < card; i++) 
            { 
                b[i] = new Branch();
            }        
        }
    
        internal RtreePage(RtreePage root, RtreePage p) 
        { 
            b = new Branch[card];
            n = 2;
            b[0] = new Branch(root.cover(), root);
            b[1] = new Branch(p.cover(), p);
            for (int i = 2; i < card; i++) 
            { 
                b[i] = new Branch();
            }        
        }

        RtreePage() {}

        public override bool RecursiveLoading() 
        {
            return false;
        }

        internal RtreePage insert(Rectangle r, IPersistent obj, int level) 
        {
            Load();
            Modify();
            if (--level != 0) 
            { 
                // not leaf page
                int i, mini = 0;
                int minIncr = Int32.MaxValue;
                int minArea = Int32.MaxValue;
                for (i = 0; i < n; i++) 
                { 
                    int area = b[i].r.Area();
                    int incr = Rectangle.JoinArea(b[i].r, r) - area;
                    if (incr < minIncr) 
                    { 
                        minIncr = incr;
                        minArea = area;
                        mini = i;
                    }
                    else if (incr == minIncr && area < minArea)
                    {
                        minArea = area;
                        mini = i;        
                    }
                }
                RtreePage p = (RtreePage)b[mini].p;
                RtreePage q = p.insert(r, obj, level);
                if (q == null) 
                { 
                    // child was not split
                    b[mini].r.Join(r);
                    return null;
                } 
                else 
                { 
                    // child was split
                    b[mini] = new Branch(p.cover(), p);
                    return addBranch(new Branch(q.cover(), q));
                }
            } 
            else 
            { 
                return addBranch(new Branch(r, obj));
            }
        }

        internal int remove(Rectangle r, IPersistent obj, int level, ArrayList reinsertList) 
        {
            Load();
            Modify();
            if (--level != 0) 
            { 
                for (int i = 0; i < n; i++) 
                { 
                    if (r.Intersects(b[i].r)) 
                    { 
                        RtreePage pg = (RtreePage)b[i].p;
                        int reinsertLevel = pg.remove(r, obj, level, reinsertList);
                        if (reinsertLevel >= 0) 
                        { 
                            if (pg.n >= minFill) 
                            { 
                                b[i] = new Branch(pg.cover(), pg);
                            } 
                            else 
                            { 
                                // not enough entries in child
                                reinsertList.Add(pg);
                                reinsertLevel = level - 1;
                                removeBranch(i);
                            }
                            return reinsertLevel;
                        }
                    }
                }
            } 
            else 
            {
                for (int i = 0; i < n; i++) 
                { 
                    if (b[i].p == obj) 
                    { 
                        removeBranch(i);
                        return 0;
                    }
                }
            }
            return -1;        
        }

       internal void find(Rectangle r, ArrayList result, int level) 
        {
            Load();
            if (--level != 0) 
            { /* this is an internal node in the tree */
                for (int i = 0; i < n; i++) 
                { 
                    if (r.Intersects(b[i].r)) 
                    {
                        ((RtreePage)b[i].p).find(r, result, level); 
                    }
                }
            } 
            else 
            { /* this is a leaf node */
                for (int i = 0; i < n; i++) 
                { 
                    if (r.Intersects(b[i].r)) 
                    { 
                        IPersistent obj = b[i].p;
                        obj.Load();
                        result.Add(obj);
                    }
                }
            }
        }

        internal void purge(int level) 
        {
            Load();
            if (--level != 0) 
            { /* this is an internal node in the tree */
                for (int i = 0; i < n; i++) 
                { 
                    ((RtreePage)b[i].p).purge(level);
                }
            }
            Deallocate();
        }
    
        internal void removeBranch(int i) 
        {
            n -= 1;
            Array.Copy(b, i+1, b, i, n-i);
        }

        internal RtreePage addBranch(Branch br) 
        { 
            if (n < card) 
            { 
                b[n++] = br;
                return null;
            } 
            else 
            { 
                return splitPage(br);
            }
        }

        internal RtreePage splitPage(Branch br) 
        { 
            int i, j, seed0 = 0, seed1 = 0;
            int[]  rectArea = new int[card+1];
            int    waste;
            int    worstWaste = Int32.MinValue;
            //
            // As the seeds for the two groups, find two rectangles which waste 
            // the most area if covered by a single rectangle.
            //
            rectArea[0] = br.r.Area();
            for (i = 0; i < card; i++) 
            { 
                rectArea[i+1] = b[i].r.Area();
            }
            Branch bp = br;
            for (i = 0; i < card; i++) 
            { 
                for (j = i+1; j <= card; j++) 
                { 
                    waste = Rectangle.JoinArea(bp.r, b[j-1].r) - rectArea[i] - rectArea[j];
                    if (waste > worstWaste) 
                    {
                        worstWaste = waste;
                        seed0 = i;
                        seed1 = j;
                    }
                }
                bp = b[i];
            }       
            byte[] taken = new byte[card];
            Rectangle group0, group1;
            int       groupArea0, groupArea1;
            int       groupCard0, groupCard1;
            RtreePage pg;

            taken[seed1-1] = 2;
            group1 = b[seed1-1].r;

            if (seed0 == 0) 
            { 
                group0 = br.r;
                pg = new RtreePage(br.p, br.r);
            } 
            else 
            { 
                group0 = b[seed0-1].r;
                pg = new RtreePage(b[seed0-1].p, group0);
                b[seed0-1] = br;
            }
            groupCard0 = groupCard1 = 1;
            groupArea0 = rectArea[seed0];
            groupArea1 = rectArea[seed1];
            //
            // Split remaining rectangles between two groups.
            // The one chosen is the one with the greatest difference in area 
            // expansion depending on which group - the rect most strongly 
            // attracted to one group and repelled from the other.
            //
            while (groupCard0 + groupCard1 < card + 1 
                && groupCard0 < card + 1 - minFill
                && groupCard1 < card + 1 - minFill)
            {
                int betterGroup = -1, chosen = -1;
                int biggestDiff = -1;
                for (i = 0; i < card; i++) 
                { 
                    if (taken[i] == 0) 
                    { 
                        int diff = (Rectangle.JoinArea(group0, b[i].r) - groupArea0)
                            - (Rectangle.JoinArea(group1, b[i].r) - groupArea1);
                        if (diff > biggestDiff || -diff > biggestDiff) 
                        { 
                            chosen = i;
                            if (diff < 0) 
                            { 
                                betterGroup = 0;
                                biggestDiff = -diff;
                            } 
                            else 
                            { 
                                betterGroup = 1;
                                biggestDiff = diff;
                            }
                        }
                    }
                }
                Debug.Assert(chosen >= 0);
                if (betterGroup == 0) 
                { 
                    group0.Join(b[chosen].r);
                    groupArea0 = group0.Area();
                    taken[chosen] = 1;
                    pg.b[groupCard0++] = b[chosen];
                } 
                else 
                {
                    groupCard1 += 1;
                    group1.Join(b[chosen].r);
                    groupArea1 = group1.Area();
                    taken[chosen] = 2;
                }
            }
            //
            // If one group gets too full, then remaining rectangle are
            // split between two groups in such way to balance cards of two groups.
            //
            if (groupCard0 + groupCard1 < card + 1) 
            { 
                for (i = 0; i < card; i++) 
                { 
                    if (taken[i] == 0) 
                    { 
                        if (groupCard0 >= groupCard1) 
                        { 
                            taken[i] = 2;
                            groupCard1 += 1;
                        } 
                        else 
                        { 
                            taken[i] = 1;
                            pg.b[groupCard0++] = b[i];               
                        }
                    }
                }
            }
            pg.n = groupCard0;
            n = groupCard1;
            for (i = 0, j = 0; i < groupCard1; j++) 
            { 
                if (taken[j] == 2) 
                {
                    b[i++] = b[j];
                }
            }
            return pg;
        }   

        internal Rectangle cover() 
        {
            Rectangle r = b[0].r;
            for (int i = 1; i < n; i++) 
            { 
                r.Join(b[i].r);
            }
            return r;
        }
    }
}
